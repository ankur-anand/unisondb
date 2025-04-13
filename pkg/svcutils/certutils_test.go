package svcutils_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/pkg/svcutils"
	"github.com/stretchr/testify/require"
)

func generateSelfSignedCert(t *testing.T, isCA bool) (certPEM, keyPEM []byte) {
	t.Helper()

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: "svcutil-test",
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(24 * time.Hour),

		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  isCA,
	}
	if isCA {
		template.KeyUsage |= x509.KeyUsageCertSign
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	require.NoError(t, err)

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	return certPEM, keyPEM
}

func writeTempFile(t *testing.T, data []byte) string {
	t.Helper()
	f, err := os.CreateTemp("", "tls-test-*")
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

func TestNewTLSCreds(t *testing.T) {
	caCert, _ := generateSelfSignedCert(t, true)
	caPath := writeTempFile(t, caCert)
	defer os.Remove(caPath)

	creds, err := svcutils.NewTLSCreds(caPath)
	require.NoError(t, err)
	require.NotNil(t, creds)
}

func TestNewMTLSCreds(t *testing.T) {
	caCert, caKey := generateSelfSignedCert(t, true)
	clientCert, clientKey := generateSelfSignedCert(t, false)

	caParsed, err := tls.X509KeyPair(caCert, caKey)
	require.NoError(t, err)

	caTemplate, err := x509.ParseCertificate(caParsed.Certificate[0])
	require.NoError(t, err)

	clientPriv, _ := tls.X509KeyPair(clientCert, clientKey)
	clientCertParsed, _ := x509.ParseCertificate(clientPriv.Certificate[0])

	signedCertDER, err := x509.CreateCertificate(rand.Reader,
		clientCertParsed, caTemplate,
		clientPriv.PrivateKey.(*rsa.PrivateKey).Public(), caParsed.PrivateKey)
	require.NoError(t, err)

	signedCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: signedCertDER})

	certPath := writeTempFile(t, signedCertPEM)
	keyPath := writeTempFile(t, clientKey)
	caPath := writeTempFile(t, caCert)

	defer os.Remove(certPath)
	defer os.Remove(keyPath)
	defer os.Remove(caPath)

	creds, err := svcutils.NewMTLSCreds(certPath, keyPath, caPath)
	require.NoError(t, err)
	require.NotNil(t, creds)
}

func TestNewTLSCreds_FileNotFound(t *testing.T) {
	_, err := svcutils.NewTLSCreds("/non/existent/ca.pem")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to create TLS credentials")
}

func TestNewMTLSCreds_ClientCertMissing(t *testing.T) {
	caPath := writeTempFile(t, []byte("invalid-ca"))
	defer os.Remove(caPath)

	_, err := svcutils.NewMTLSCreds("/does/not/exist.crt", "/does/not/exist.key", caPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to load client cert/key")
}
