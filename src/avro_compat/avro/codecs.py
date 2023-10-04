from cavro import Codec, _NullCodec, _SnappyCodec, _DeflateCodec
from .errors import UnsupportedCodec
from typing import Type


KNOWN_CODECS = {
    "null": _NullCodec,
    "snappy": _SnappyCodec,
    "deflate": _DeflateCodec,
}


def get_codec(codec_name: str) -> Type[Codec]:
    try:
        return KNOWN_CODECS[codec_name]
    except KeyError:
        raise UnsupportedCodec(f"Unsupported codec: {codec_name}. (Is it installed?)")
