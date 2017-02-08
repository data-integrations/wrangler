# Message Digest creation

HASH directive generating a message digest.

## Syntax
```
  hash <column> <algorithm> [encode]
```

```column``` is the name of the column to which the hashing ```algorithm```
needs to be applied.

If ```encode``` is set to 'true', the hashed digest is encoded as HEX with left padding zero. By default, ```encode```
is set to 'true' to disable HEX encoding set it to false.

## Usage Notes

The HASH directive when applied on a ```column``` will replace the content of the column with the message hash.
No new columns are created with the application of this directive.

Following are algorithms supported by HASH directive

* SHA
* SHA-384
* SHA-512
* MD5
* SHA-256
* MD2
* KECCAK-224
* SHA3-512
* RIPEMD160
* KECCAK-512
* RIPEMD128
* BLAKE2B-384
* Skein-512-256
* WHIRLPOOL
* Skein-512-224
* Skein-1024-1024
* SHA-1
* SHA-384
* Skein-512-512
* GOST3411
* Skein-1024-512
* Skein-256-256
* MD5
* MD4
* MD2
* Skein-256-224
* SM3
* Skein-512-160
* BLAKE2B-256
* Skein-512-128
* RIPEMD320
* GOST3411-2012-256
* BLAKE2B-512
* SHA-256
* SHA-224
* SHA3-384
* Skein-256-160
* Skein-256-128
* KECCAK-384
* GOST3411-2012-512
* TIGER
* SHA-512
* SHA-512/256
* SHA-512/224
* RIPEMD256
* BLAKE2B-160
* Skein-512-384
* Skein-1024-384
* Tiger
* SHA3-256
* KECCAK-288
* SHA3-224
* KECCAK-256

## Example

Let's assume the following record

```
  {
    "message" : "secret message"
  }
```

applying the directive

```
  hash message SHA3-384
```

Will generate the message digest and following is the modified record after application of this directive.

> Note: By default encoding is on

```
  {
    "message" : "9cc25835d1ef78b4cd8b36a0c4ad636a6094fbb944b1d880f21c7129a645e819d3be987e8ae2f0f8d6cbebb8452419ef"
  }
```

The column ```message``` is replaced with digest applying the algorithm SHA3-384. The type of the column is String.

But, when ```encode``` is set to false, the output column type is a byte array.