package cliapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	manifeststore "github.com/ankur-anand/isledb/manifest"
	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/ankur-anand/unisondb/dbkernel"
	udbinternal "github.com/ankur-anand/unisondb/internal"
	"github.com/ankur-anand/unisondb/internal/services/streamer"
	"github.com/ankur-anand/unisondb/pkg/raftcluster"
	"golang.org/x/sync/errgroup"
)

type BlobStoreStreamerService struct {
	deps          *Dependencies
	cfg           config.BlobStoreStreamingConfig
	enabled       bool
	flushInterval time.Duration
	stores        map[string]*blobstore.Store
	manifests     map[string]*manifeststore.Store
}

type blobStoreStreamerTerm struct {
	cancel   context.CancelFunc
	streamer *streamer.BlobStoreStreamer
	done     <-chan error
}

func (b *BlobStoreStreamerService) Name() string {
	return "blobstore-streamer"
}

func (b *BlobStoreStreamerService) Setup(ctx context.Context, deps *Dependencies) error {
	b.deps = deps
	b.cfg = deps.Config.BlobStoreStreaming
	b.stores = make(map[string]*blobstore.Store)
	b.manifests = make(map[string]*manifeststore.Store)

	if !b.cfg.Enabled {
		b.enabled = false
		return nil
	}

	if deps.Mode == modeReplica || deps.Mode == modeRelay {
		slog.Info("[unisondb.cliapp]",
			slog.String("event_type", "blobstore.streamer.disabled"),
			slog.String("reason", "read_only_mode"),
			slog.String("mode", deps.Mode))
		b.enabled = false
		return nil
	}

	if b.cfg.FlushInterval != "" {
		interval, err := time.ParseDuration(b.cfg.FlushInterval)
		if err != nil {
			return fmt.Errorf("blobstore streamer: parse flush interval: %w", err)
		}
		b.flushInterval = interval
	}

	if len(b.cfg.Namespaces) == 0 {
		return errors.New("blobstore streamer: enabled but no namespaces configured")
	}

	for namespace, nsCfg := range b.cfg.Namespaces {
		if _, ok := deps.Engines[namespace]; !ok {
			b.closeStores()
			return fmt.Errorf("blobstore streamer: namespace %q not found in engines", namespace)
		}
		if nsCfg.BucketURL == "" {
			b.closeStores()
			return fmt.Errorf("blobstore streamer: bucket_url not configured for namespace %q", namespace)
		}

		store, err := streamer.OpenNamespaceBlobStore(ctx, nsCfg.BucketURL, nsCfg.BasePrefix, namespace)
		if err != nil {
			b.closeStores()
			return fmt.Errorf("blobstore streamer: open store for %q: %w", namespace, err)
		}

		b.stores[namespace] = store
		b.manifests[namespace] = manifeststore.NewStore(store)
	}

	b.enabled = true
	return nil
}

func (b *BlobStoreStreamerService) Run(ctx context.Context) error {
	if !b.enabled {
		return nil
	}

	if b.deps.Config.RaftConfig.Enabled {
		if b.deps.RaftService == nil {
			return errors.New("blobstore streamer: raft service unavailable")
		}
		return b.runWithRaft(ctx)
	}

	return b.runStandalone(ctx)
}

func (b *BlobStoreStreamerService) Close(ctx context.Context) error {
	return b.closeStores()
}

func (b *BlobStoreStreamerService) runStandalone(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, namespace := range b.sortedNamespaces() {
		namespace := namespace
		g.Go(func() error {
			return b.streamNamespace(ctx, namespace)
		})
	}
	return g.Wait()
}

func (b *BlobStoreStreamerService) runWithRaft(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, namespace := range b.sortedNamespaces() {
		namespace := namespace
		cluster := b.deps.RaftService.GetCluster(namespace)
		if cluster == nil {
			return fmt.Errorf("blobstore streamer: raft cluster missing for namespace %q", namespace)
		}

		g.Go(func() error {
			return b.watchLeadership(ctx, namespace, cluster)
		})
	}
	return g.Wait()
}

// nolint: gocognit
func (b *BlobStoreStreamerService) watchLeadership(ctx context.Context, namespace string, cluster *raftcluster.Cluster) error {
	var current *blobStoreStreamerTerm
	leaderCh := cluster.LeaderCh()

	stopCurrent := func() error {
		if current == nil {
			return nil
		}

		current.cancel()
		closeErr := current.streamer.Close()
		doneErr := <-current.done
		current = nil

		if closeErr != nil {
			return closeErr
		}
		if isBenignBlobStoreStreamerError(doneErr) {
			return nil
		}
		return doneErr
	}

	if cluster.IsLeader() {
		term, err := b.startTerm(ctx, namespace)
		if err != nil {
			return err
		}
		current = term
	}

	for {
		var doneCh <-chan error
		if current != nil {
			doneCh = current.done
		}

		select {
		case isLeader, ok := <-leaderCh:
			if !ok {
				return stopCurrent()
			}

			if err := stopCurrent(); err != nil {
				return fmt.Errorf("blobstore streamer: stop term for %q: %w", namespace, err)
			}
			if !isLeader {
				continue
			}

			term, err := b.startTerm(ctx, namespace)
			if err != nil {
				return err
			}
			current = term

		case err := <-doneCh:
			current = nil
			if isBenignBlobStoreStreamerError(err) {
				continue
			}
			return fmt.Errorf("blobstore streamer: term failed for %q: %w", namespace, err)

		case <-ctx.Done():
			if err := stopCurrent(); err != nil {
				return err
			}
			return ctx.Err()
		}
	}
}

func (b *BlobStoreStreamerService) startTerm(ctx context.Context, namespace string) (*blobStoreStreamerTerm, error) {
	startLSN, err := b.lastWrittenLSN(ctx, namespace)
	if err != nil {
		return nil, fmt.Errorf("blobstore streamer: last written lsn for %q: %w", namespace, err)
	}

	termCtx, cancel := context.WithCancel(ctx)
	errGrp, streamCtx := errgroup.WithContext(termCtx)
	engine, ok := b.deps.Engines[namespace]
	if !ok || engine == nil {
		cancel()
		return nil, fmt.Errorf("blobstore streamer: engine not configured for %q", namespace)
	}
	s, err := streamer.NewBlobStoreStreamer(
		streamCtx,
		errGrp,
		map[string]*dbkernel.Engine{namespace: engine},
		map[string]*blobstore.Store{namespace: b.stores[namespace]},
		streamer.BlobStoreStreamerConfig{
			FlushInterval: b.flushInterval,
		},
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("blobstore streamer: create streamer for %q: %w", namespace, err)
	}

	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "blobstore.streamer.term.started"),
		slog.String("namespace", namespace),
		slog.Uint64("start_lsn", startLSN))

	done := make(chan error, 1)
	go func() {
		streamErr := s.StreamNamespace(streamCtx, namespace, startLSN)
		waitErr := errGrp.Wait()
		done <- firstNonNil(streamErr, waitErr)
		close(done)
	}()

	return &blobStoreStreamerTerm{
		cancel:   cancel,
		streamer: s,
		done:     done,
	}, nil
}

func (b *BlobStoreStreamerService) streamNamespace(ctx context.Context, namespace string) error {
	term, err := b.startTerm(ctx, namespace)
	if err != nil {
		return err
	}
	defer term.cancel()
	defer term.streamer.Close()

	err = <-term.done
	if isBenignBlobStoreStreamerError(err) {
		return nil
	}
	return err
}

func (b *BlobStoreStreamerService) lastWrittenLSN(ctx context.Context, namespace string) (uint64, error) {
	manifestStore, ok := b.manifests[namespace]
	if !ok || manifestStore == nil {
		return 0, fmt.Errorf("manifest store not configured for namespace %q", namespace)
	}

	current, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		return 0, err
	}
	if current == nil || current.MaxCommittedLSN == nil {
		return 0, nil
	}
	return *current.MaxCommittedLSN, nil
}

func (b *BlobStoreStreamerService) sortedNamespaces() []string {
	namespaces := make([]string, 0, len(b.cfg.Namespaces))
	for namespace := range b.cfg.Namespaces {
		namespaces = append(namespaces, namespace)
	}
	slices.Sort(namespaces)
	return namespaces
}

func (b *BlobStoreStreamerService) closeStores() error {
	var errs []error
	for namespace, store := range b.stores {
		if store == nil {
			continue
		}
		if err := store.Close(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", namespace, err))
		}
	}
	return errors.Join(errs...)
}

func isBenignBlobStoreStreamerError(err error) bool {
	if err == nil {
		return true
	}
	return errors.Is(err, context.Canceled) || err.Error() == udbinternal.GracefulShutdownMsg
}

func firstNonNil(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

var _ Service = (*BlobStoreStreamerService)(nil)
