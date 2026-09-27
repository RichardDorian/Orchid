//! Test certificates.

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};

use crate::tls::TlsMaterial;

/// A throwaway cluster CA.
pub struct TestPki {
    issuer: Issuer<'static, KeyPair>,
    ca: String,
}

impl TestPki {
    pub fn new() -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("valid parameters");
        params
            .distinguished_name
            .push(DnType::CommonName, "orchid-test-ca");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let key = KeyPair::generate().expect("key generation");
        let ca = params.self_signed(&key).expect("self signed CA").pem();
        Self {
            issuer: Issuer::new(params, key),
            ca,
        }
    }

    /// Issues a certificate valid for `localhost` and `127.0.0.1`, usable by
    /// both clients and servers.
    pub fn issue(&self, common_name: &str) -> TlsMaterial {
        let mut params =
            CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
                .expect("valid parameters");
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key = KeyPair::generate().expect("key generation");
        let certificate = params
            .signed_by(&key, &self.issuer)
            .expect("signed certificate");
        TlsMaterial {
            certificate: certificate.pem().into_bytes(),
            private_key: key.serialize_pem().into_bytes(),
            ca: self.ca.clone().into_bytes(),
        }
    }
}

impl Default for TestPki {
    fn default() -> Self {
        Self::new()
    }
}
