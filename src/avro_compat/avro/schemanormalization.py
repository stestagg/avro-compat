import hashlib
from cavro import Rabin


def Fingerprint(parsing_normal_form_schema, fingerprint_algorithm_name, _hex=False):
    if fingerprint_algorithm_name in ("rabin", "CRC-64-AVRO"):
        hasher = Rabin()
    else:
        try:
            hasher = hashlib.new(fingerprint_algorithm_name)
        except ValueError as e1:
            try:
                hasher = hashlib.new(fingerprint_algorithm_name.lower().replace("-", ""))
            except ValueError:
                raise e1
    hasher.update(parsing_normal_form_schema.encode())
    if _hex:
        return hasher.hexdigest()
    else:
        return hasher.digest()


def FingerprintAlgorithmNames():
    return {"rabin", "CRC-64-AVRO", "MD5", "SHA-1", "SHA-256"} | set(hashlib.algorithms_guaranteed)


def ToParsingCanonicalForm(schema):
    return schema.canonical_form
