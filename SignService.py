import json
import base64
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography import x509
from cryptography.x509 import Certificate
import datetime
import hashlib


class SignService:
    def __init__(
        self,
        invoice_data: dict,
        private_key: RSAPrivateKey,
        cert: Certificate
    ):
        self.invoice_data = invoice_data
        self.invoice_data_minified = json.dumps(invoice_data, separators=(",", ":"))
        self.private_key = private_key
        self.cert = cert
        
    def digest_doc(self) -> str:
        hash_obj = hashlib.sha256(self.invoice_data_minified.encode('utf-8')).digest()
        return base64.b64encode(hash_obj).decode('utf-8')
    
    def sign_doc(self) -> str:
        hash_obj = hashlib.sha256(self.invoice_data_minified.encode('utf-8')).digest()
        signature = self.private_key.sign(
            hash_obj,
            padding.PKCS1v15(),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')
    
    def digest_cert(self) -> str:
        hash_obj = hashlib.sha256(self.cert.public_bytes(encoding=serialization.Encoding.PEM)).digest()
        return base64.b64encode(hash_obj).decode('utf-8')

    def gen_sign_props(self) -> dict:
        self.sign_props = {
            "Target":"signature",
            "SignedProperties":[{
                "Id":"id-xades-signed props",
                "SignedSignatureProperties":[{
                    "SigningTime":[{"_":datetime.datetime.now(datetime.timezone.utc).isoformat()}],
                    "SigningCertificate":[{
                        "Cert":[{
                            "CertDigest":[{
                                "DigestMethod":[{"_":"","Algorithm":"http://www.w3.org/2001/04/xmlenc#sha256"}],
                                "DigestValue":[{"_":self.digest_cert()}]}],
                            "IssuerSerial":[{
                                "X509IssuerName":[{"_":self.cert.issuer.rfc4514_string()}],
                                "X509SerialNumber":[{"_":self.cert.serial_number}]
                            }]
                        }]
                    }]
                }]
            }]
        }
        return self.sign_props
    
    def digest_sign_props(self) -> str:
        signedprops = json.dumps(self.sign_props, separators=(",", ":"))
        signedpropshash = hashlib.sha256(signedprops.encode()).digest()
        return base64.b64encode(signedpropshash).decode()
    
    def gen_sign(self) -> dict:
        self.gen_sign_props()
        self.sign = {
            "UBLExtensions": [{
                "UBLExtension": [{
                    "ExtensionURI": [{"_": "urn:oasis:names:specification:ubl:dsig:enveloped:xades"}],
                    "ExtensionContent": [{
                        "UBLDocumentSignatures": [{
                            "SignatureInformation": [{
                                "ID": [{"_": "urn:oasis:names:specification:ubl:signature:1"}],
                                "ReferencedSignatureID": [{"_": "urn:oasis:names:specification:ubl:signature:Invoice"}],
                                "Signature": [{
                                    "Id": "signature",
                                    "Object": [{
                                        "QualifyingProperties": [{
                                            "Target": "signature",
                                            "SignedProperties": [{
                                                "Id": "id-xades-signed-props",
                                                "SignedSignatureProperties": [{
                                                    # * Assign UTC timestamp
                                                    "SigningTime": [{"_": datetime.datetime.now(datetime.timezone.utc).isoformat()}],
                                                    "SigningCertificate": [{
                                                        "Cert": [{
                                                            "CertDigest": [{
                                                                "DigestMethod": [{
                                                                    "_": "",
                                                                    "Algorithm": "http://www.w3.org/2001/04/xmlenc#sha256"
                                                                }],
                                                                # * Assign DigestValue
                                                                "DigestValue": [{"_": self.digest_cert()}]
                                                            }],
                                                            "IssuerSerial": [{
                                                                # * Assign Issuer Name and Serial Number
                                                                "X509IssuerName": [{"_": self.cert.issuer.rfc4514_string()}],
                                                                "X509SerialNumber": [{"_": self.cert.serial_number}]
                                                            }]
                                                        }]
                                                    }]
                                                }]
                                            }]
                                        }]
                                    }],
                                    "KeyInfo": [{
                                        "X509Data": [{
                                            # * Assign X509Certificate, X509SubjectName, X509IssuerSerial
                                            "X509Certificate": [{
                                                "_": base64.b64encode(self.cert.public_bytes(encoding=serialization.Encoding.PEM)).decode()
                                            }],
                                            # * Assign X509SubjectName
                                            "X509SubjectName": [{
                                                "_": self.cert.subject.rfc4514_string()
                                            }],
                                            # * Assign X509IssuerSerial
                                            "X509IssuerSerial": [{
                                                "X509IssuerName": [{"_": self.cert.issuer.rfc4514_string()}],
                                                "X509SerialNumber": [{
                                                    "_": self.cert.serial_number
                                                }]
                                            }]
                                        }]
                                    }],
                                    # * Assign SignatureValue
                                    "SignatureValue": [{"_": self.sign_doc()}],
                                    "SignedInfo": [{
                                        "SignatureMethod": [{
                                            "_": "",
                                            "Algorithm": "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"
                                        }],
                                        "Reference": [{
                                            "Type": "http://uri.etsi.org/01903/v1.3.2#SignedProperties",
                                            "URI": "#id-xades-signed-props",
                                            "DigestMethod": [{
                                                "_": "",
                                                "Algorithm": "http://www.w3.org/2001/04/xmlenc#sha256"
                                            }],
                                            "DigestValue": [{
                                                # * Assign PropsDigest
                                                "_": self.digest_sign_props()
                                            }]
                                        },
                                        {
                                            "Type": "",
                                            "URI": "",
                                            "DigestMethod": [{
                                                "_": "",
                                                "Algorithm": "http://www.w3.org/2001/04/xmlenc#sha256"
                                            }],
                                            "DigestValue": [{
                                                # * Assign DocDigest
                                                "_": self.digest_doc()
                                            }]
                                        }]
                                    }]
                                }]
                            }]
                        }]
                    }]
                }]
            }],
            "Signature": [{
                "ID": [{"_": "urn:oasis:names:specification:ubl:signature:Invoice"}],
                "SignatureMethod": [{"_": "urn:oasis:names:specification:ubl:dsig:enveloped:xades"}]
            }]
        }
        
        return self.sign


with open("invoice.json", "r") as f:
    invoice_data = json.load(f)
    
with open("private.pem", "rb") as f:
    private_key = serialization.load_pem_private_key(
        f.read(),
        password=None
    )

with open("certificate.pem", "r") as f:
    cert_str = f.read()
    cert = x509.load_pem_x509_certificate(cert_str.encode())
    
sign_service = SignService(invoice_data, private_key, cert)
sign_service.gen_sign()

