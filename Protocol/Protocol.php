<?php
namespace Kafka\Protocol;

/**
 * Kafka Protocol since Kafka v0.10
 *
 */
abstract class Protocol
{
    /**
     * protocol version
     */
    const API_VRESION = 1;

    /**
     * allow backwards compatible evolution of the message binary format.
     */
    const MESSAGE_MAGIC = 0;

    /**
     * message not compression
     */
    const COMPRESSION_NONE = 0;

    /**
     * message gzip compression
     */
    const COMPRESSION_GZIP = 1;

    /**
     * message snappy compression
     */
    const COMPRESSION_SNAPPY = 2;

    /**
     * pack int32 type
     */
    const PACK_INT32 = 0;

    /**
     * pack int16 type
     */
    const PACK_INT16 = 1;

    /**
     * protocol APIs request code
     */
    const PRODUCE_REQUEST = 0;
    const FETCH_REQUEST   = 1;
    const OFFSET_REQUEST  = 2;
    const METADATA_REQUEST      = 3;
    const OFFSET_COMMIT_REQUEST = 8;
    const OFFSET_FETCH_REQUEST  = 9;
    const CONSUMER_METADATA_REQUEST = 10;

    /**
     * unpack/pack bit
     */
    const BIT_B64 = 'N2';
    const BIT_B32 = 'N';
    const BIT_B16 = 'n';
    const BIT_B8  = 'C';

    /**
     * stream
     * @var mixed
     * @access protected
     */
    protected $stream = null;

    public function __construct(\Kafka\Socket $stream)
    {
        $this->stream = $stream;
    }

    public static function KafkaHex2bin($string)
    {
        if (function_exists('\hex2bin')) {
            return \hex2bin($string);
        } else {
            $bin = "";
            $len = strlen($string);
            for ($i = 0; $i < $len; $i = $i + 2) {
                $bin .= pack("H*", substr($string, $i, 2));
            }
        }
        return $bin;
    }

    public static function pack($type, $data)
    {
        if($type == self::BIT_B64) {
            if ($data == -1) {
                $data = self::KafkaHex2bin('ffffffffffffffff');
            } elseif ($data == -2) {
                $data = self::KafkaHex2bin('fffffffffffffffe');
            } else {
                $left  = 0xffffffff00000000;
                $right = 0x00000000ffffffff;
                $l = ($data & $left) >> 32;
                $r = $data & $right;
                $data = pack($type, $l, $r);
            }
        } else {
            $data = pack($type, $data);
        }
        return $data;
    }

    public static function unpack($type, $bytes)
    {
        self::checkLen($type, $bytes);
        if ($type == self::BIT_B64) {
            $set = unpack($type, $bytes);
            $original = ($set[1] & 0xFFFFFFFF) << 32 | ($set[2] & 0xFFFFFFFF);
            return $original;
        } else {
            return unpack($type, $bytes);
        }
    }

    protected static function checkLen($type, $bytes)
    {
        $len = 0;
        switch ($type) {
            case self::BIT_B64:
                $len = 8;
                break;
            case self::BIT_B32:
                $len = 4;
            case self::BIT_B16:
                $len = 2;
            case self::BIT_B8:
                $len = 1;
        }
        if (strlen($bytes) != $len) {
            throw new Exception(('unpack failed. string(raw) length is ' . strlen($bytes) . ' , TO ' . $type);
        }
    }
}
