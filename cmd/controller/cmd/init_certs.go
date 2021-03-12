package cmd

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"io/ioutil"
	"path/filepath"

	config2 "github.com/flyteorg/flytepropeller/pkg/controller/config"

	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/spf13/cobra"
)

const webhookName = "flyte-pod-webhook.flyte.org"

var certsCmd = &cobra.Command{
	Use:     "init-certs",
	Aliases: []string{"init-cert"},
	Short:   "Generates CA, Cert and cert key and saves them into",
	Example: "flytepropeller webhook init-certs",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runCertsCmd(context.Background(), config2.GetConfig())
	},
}

func init() {
	webhookCmd.AddCommand(certsCmd)
}

func runCertsCmd(ctx context.Context, cfg *config2.Config) error {
	dirPath := cfg.Webhook.CertDir
	if len(dirPath) == 0 {
		dirPath = filepath.Join(fmt.Sprintf("%v", os.PathSeparator), "etc", "webhooks", "certs")
	}

	_, _, err := createCerts(ctx, dirPath)
	if err != nil {
		return err
	}

	return nil
}

func createCerts(_ context.Context, dirPath string) (caPEM, serverCertPEM *bytes.Buffer, err error) {
	var serverPrivKeyPEM *bytes.Buffer
	// CA config
	caRequest := &x509.Certificate{
		SerialNumber: big.NewInt(2020),
		Subject: pkix.Name{
			Organization: []string{"flyte.org"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(1, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// CA private key
	caPrivKey, err := rsa.GenerateKey(cryptorand.Reader, 4096)
	if err != nil {
		return nil, nil, err
	}

	// Self signed CA certificate
	caCert, err := x509.CreateCertificate(cryptorand.Reader, caRequest, caRequest, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, nil, err
	}

	// PEM encode CA cert
	caPEM = new(bytes.Buffer)
	err = pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCert,
	})
	if err != nil {
		return nil, nil, err
	}

	dnsNames := []string{"flyte-pod-webhook",
		"flyte-pod-webhook.flyte", "flyte-pod-webhook.flyte.svc"}
	commonName := "flyte-pod-webhook.flyte.svc"

	// server cert config
	certRequest := &x509.Certificate{
		DNSNames:     dnsNames,
		SerialNumber: big.NewInt(1658),
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"flyte.org"},
		},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(1, 0, 0),
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	// server private key
	serverPrivKey, err := rsa.GenerateKey(cryptorand.Reader, 4096)
	if err != nil {
		return nil, nil, err
	}

	// sign the server cert
	cert, err := x509.CreateCertificate(cryptorand.Reader, certRequest, caRequest, &serverPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, nil, err
	}

	// PEM encode the  server cert and key
	serverCertPEM = new(bytes.Buffer)
	err = pem.Encode(serverCertPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert,
	})

	if err != nil {
		return nil, nil, fmt.Errorf("failed to Encode CertPEM. Error: %w", err)
	}

	serverPrivKeyPEM = new(bytes.Buffer)
	err = pem.Encode(serverPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(serverPrivKey),
	})

	if err != nil {
		return nil, nil, fmt.Errorf("failed to Encode Cert Private Key. Error: %w", err)
	}

	err = os.MkdirAll(dirPath, 0755)
	if err != nil {
		return nil, nil, err
	}

	err = WriteFile(filepath.Join(dirPath, "ca.crt"), caPEM)
	if err != nil {
		return nil, nil, err
	}

	err = WriteFile(filepath.Join(dirPath, "tls.crt"), serverCertPEM)
	if err != nil {
		return nil, nil, err
	}

	err = WriteFile(filepath.Join(dirPath, "tls.key"), serverPrivKeyPEM)
	if err != nil {
		return nil, nil, err
	}

	return caPEM, serverCertPEM, nil
}

// WriteFile writes data in the file at the given path
func WriteFile(filepath string, sCert *bytes.Buffer) error {
	f, err := os.Create(filepath)
	if err != nil {
		if f != nil {
			err2 := f.Close()
			if err2 != nil {
				return fmt.Errorf("failed to create and close file. Create Error: %w. Close Error: %v", err, err2)
			}
		}

		return fmt.Errorf("failed to create file. Error: %w", err)
	}

	_, err = f.Write(sCert.Bytes())
	if err != nil {
		err2 := f.Close()
		if err2 != nil {
			return fmt.Errorf("failed to write and close file. Write Error: %w. Close Error: %v", err, err2)
		}

		return err
	}

	return nil
}

// ReadFile reads file contents given a path
func ReadFile(filepath string) (*bytes.Buffer, error) {
	f, err := os.Open(filepath)
	if err != nil {
		if f != nil {
			err2 := f.Close()
			if err2 != nil {
				return nil, fmt.Errorf("failed to create and close file. Create Error: %w. Close Error: %v", err, err2)
			}
		}

		return nil, fmt.Errorf("failed to create file. Error: %w", err)
	}

	buf, err := ioutil.ReadAll(f)
	if err != nil {
		err2 := f.Close()
		if err2 != nil {
			return nil, fmt.Errorf("failed to write and close file. Write Error: %w. Close Error: %v", err, err2)
		}

		return nil, err
	}

	return bytes.NewBuffer(buf), nil
}
