package cmd

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"

	"github.com/flyteorg/flytestdlib/logger"
	"k8s.io/apimachinery/pkg/api/errors"

	"github.com/flyteorg/flytepropeller/pkg/controller/config"

	corev1 "k8s.io/api/core/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/flyteorg/flytepropeller/pkg/webhook"

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

const (
	CaCertKey            = "ca.crt"
	ServerCertKey        = "tls.crt"
	ServerCertPrivateKey = "tls.key"
)

// initCertsCmd initializes x509 TLS Certificates and saves them to a secret.
var initCertsCmd = &cobra.Command{
	Use:     "init-certs",
	Aliases: []string{"init-cert"},
	Short:   "Generates CA, Cert and cert key and saves them into",
	Example: "flytepropeller webhook init-certs",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runCertsCmd(context.Background(), config.GetConfig(), webhook.GetConfig())
	},
}

type webhookCerts struct {
	// base64 Encoded CA Cert
	CaPEM *bytes.Buffer
	// base64 Encoded Server Cert
	ServerPEM *bytes.Buffer
	// base64 Encoded Server Cert Key
	PrivateKeyPEM *bytes.Buffer
}

func init() {
	webhookCmd.AddCommand(initCertsCmd)
}

func runCertsCmd(ctx context.Context, propellerCfg *config.Config, cfg *webhook.Config) error {
	logger.Infof(ctx, "Issuing certs")
	certs, err := createCerts()
	if err != nil {
		return err
	}

	podNamespace, found := os.LookupEnv(PodNamespaceEnvVar)
	if !found {
		podNamespace = podDefaultNamespace
	}

	kubeClient, _, err := getKubeConfig(ctx, propellerCfg)
	if err != nil {
		return err
	}

	logger.Infof(ctx, "Creating secret [%v] in Namespace [%v]", cfg.SecretName, podNamespace)
	err = createWebhookSecret(ctx, podNamespace, cfg, certs, kubeClient.CoreV1().Secrets(podNamespace))
	if err != nil {
		return err
	}

	return nil
}

func createWebhookSecret(ctx context.Context, namespace string, cfg *webhook.Config, certs webhookCerts, secretsClient v1.SecretInterface) error {
	isImmutable := true
	_, err := secretsClient.Create(ctx, &corev1.Secret{
		ObjectMeta: v12.ObjectMeta{
			Name:      cfg.SecretName,
			Namespace: namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			CaCertKey:            certs.CaPEM.Bytes(),
			ServerCertKey:        certs.ServerPEM.Bytes(),
			ServerCertPrivateKey: certs.PrivateKeyPEM.Bytes(),
		},
		Immutable: &isImmutable,
	}, v12.CreateOptions{})

	if errors.IsAlreadyExists(err) {
		// TODO: Maybe get the secret and validate it has all the required keys?
		logger.Infof(ctx, "A secret already exists with the same name. Ignoring creating secret.")
		return nil
	}

	logger.Infof(ctx, "Created secret [%v]", cfg.SecretName)

	return err
}

func createCerts() (certs webhookCerts, err error) {
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
	caPrivateKey, err := rsa.GenerateKey(cryptorand.Reader, 4096)
	if err != nil {
		return webhookCerts{}, err
	}

	// Self signed CA certificate
	caCert, err := x509.CreateCertificate(cryptorand.Reader, caRequest, caRequest, &caPrivateKey.PublicKey, caPrivateKey)
	if err != nil {
		return webhookCerts{}, err
	}

	// PEM encode CA cert
	caPEM := new(bytes.Buffer)
	err = pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCert,
	})
	if err != nil {
		return webhookCerts{}, err
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
	serverPrivateKey, err := rsa.GenerateKey(cryptorand.Reader, 4096)
	if err != nil {
		return webhookCerts{}, err
	}

	// sign the server cert
	cert, err := x509.CreateCertificate(cryptorand.Reader, certRequest, caRequest, &serverPrivateKey.PublicKey, caPrivateKey)
	if err != nil {
		return webhookCerts{}, err
	}

	// PEM encode the  server cert and key
	serverCertPEM := new(bytes.Buffer)
	err = pem.Encode(serverCertPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert,
	})

	if err != nil {
		return webhookCerts{}, fmt.Errorf("failed to Encode CertPEM. Error: %w", err)
	}

	serverPrivKeyPEM := new(bytes.Buffer)
	err = pem.Encode(serverPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(serverPrivateKey),
	})

	if err != nil {
		return webhookCerts{}, fmt.Errorf("failed to Encode Cert Private Key. Error: %w", err)
	}

	return webhookCerts{
		CaPEM:         caPEM,
		ServerPEM:     serverCertPEM,
		PrivateKeyPEM: serverPrivKeyPEM,
	}, nil
}
