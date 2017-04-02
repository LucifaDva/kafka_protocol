<?php
namespace Kafka\Protocol;
include("Protocol.php");
/**
* Kafka protocol since Kafka v0.10
*
* @package
* @version 0.0.1
* @copyright Copyleft
* @author lucifa diva
*/

class Request extends Protocol
{
    /**
     * Produce request Protocol
     * v0, v1 (supported in 0.9.0 or later) and v2 (supported in 0.10.0 or later)
     * ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
     *   RequiredAcks => int16
     *   Timeout => int32
     *   Partition => int32
     *   MessageSetSize => int32
     * @param array $payloads
     * @static
     * @access public
     * @return void
     */
    public function produceRequest($payloads, $compression = self::COMPRESSION_NONE)
    {
        if (!isset($payloads['data'])) {
            throw new \Kafka\Exception('given procude data invalid. `data` is undefined.');
        }

        if (!isset($payloads['required_ack'])) {
            // default server will not send any response,(this is the only case where the server will not reply to a request)
            $payloads['required_ack'] = 0;
        }

        if (!isset($payloads['timeout'])) {
            $payloads['timeout'] = 100; // default timeout 100ms
        }

        $header = self::requestHeader('kafka-php', 0, self::PRODUCE_REQUEST);
        $data   = self::pack(self::BIT_B16, $payloads['required_ack']);
        $data  .= self::pack(self::BIT_B32, $payloads['timeout']);
        $data  .= self::encodeArray($payloads['data'], array(__CLASS__, '_encodeProduceTopic'), $compression);
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $this->stream->write($data);
    }

    /**
     * Metadata Request Protocol
     *
     * TopicMetadataRequest => [TopicName]
     *   TopicName => string
     * @param array $topics
     * @access public
     * @return string
     */
    public function metadataRequest($topics)
    {
        if (!is_array($topics)) {
            $topics = array($topics);
        }

        foreach ($topics as $topic) {
            if (!is_string($topic)) {
                throw new \Kafka\Exception('request metadata topic array have invalid value. ');
            }
        }

        $header = self::requestHeader('kafka-php', 0, self::METADATA_REQUEST);
        $data   = self::encodeArray($topics, array(__CLASS__, 'encodeString'), self::PACK_INT16);
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $this->stream->write($data);
    }

    /**
     * Group Coordinator Request Protocol
     * 
     * GroupCoordinatorRequest => GroupId
     *   GroupId => string
     */
    public function consumerMetadataRequest($consumer_group) {
        if (!is_string($consumer_group)) {
            return false;
        }
        $header = self::requestHeader('kafka-php', 0, self::CONSUMER_METADATA_REQUEST);
        $data = self::encodeString($consumer_group, self::PACK_INT16);
        $data = self::encodeString($header.$data, self::PACK_INT32);
        return $this->stream->write($data);
    }

    /**
     * Fetch Request Protocol
     *
     * FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
     *   ReplicaId => int32
     *   MaxWaitTime => int32
     *   MinBytes => int32
     *   TopicName => string
     *   Partition => int32
     *   FetchOffset => int64
     *   MaxBytes => int32
     * @param array $payloads
     * @access public
     * @return string
     */
    public function fetchRequest($payloads)
    {
        if (!isset($payloads['data'])) {
            throw new \Kafka\Exception('given fetch kafka data invalid. `data` is undefined.');
        }

        if (!isset($payloads['replica_id'])) {
            $payloads['replica_id'] = -1;
        }

        if (!isset($payloads['max_wait_time'])) {
            $payloads['max_wait_time'] = 100; // default timeout 100ms
        }

        if (!isset($payloads['min_bytes'])) {
            $payloads['min_bytes'] = 64 * 1024; // 64k
        }

        $header = self::requestHeader('kafka-php', 0, self::FETCH_REQUEST);
        $data   = self::pack(self::BIT_B32, $payloads['replica_id']);
        $data  .= self::pack(self::BIT_B32, $payloads['max_wait_time']);
        $data  .= self::pack(self::BIT_B32, $payloads['min_bytes']);
        $data  .= self::encodeArray($payloads['data'], array(__CLASS__, '_encodeFetchTopic'));
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $this->stream->write($data);
    }

    /**
     * Offset Request Protocol
     *
     * v0
     * ListOffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
     *   ReplicaId => int32
     *   TopicName => string
     *   Partition => int32
     *   Time => int64
     *   MaxNumberOfOffsets => int32
     *  
     * v1 (supported in 0.10.1.0 and later)
     * ListOffsetRequest => ReplicaId [TopicName [Partition Time]]
     *   ReplicaId => int32
     *   TopicName => string
     *   Partition => int32
     *   Time => int64
     * @param array $payloads
     * @access public
     * @return string
     */
    public function offsetRequest($payloads)
    {
        if (!isset($payloads['data'])) {
            throw new \Kafka\Exception('given offset data invalid. `data` is undefined.');
        }

        if (!isset($payloads['replica_id'])) {
            $payloads['replica_id'] = -1;
        }

        $header = self::requestHeader('kafka-php', 0, self::OFFSET_REQUEST);
        $data   = self::pack(self::BIT_B32, $payloads['replica_id']);
        $data  .= self::encodeArray($payloads['data'], array(__CLASS__, '_encodeOffsetTopic'));
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $this->stream->write($data);
    }

    /**
     * Offset commit Request
     * v1 (supported in 0.8.2 or later)
     * OffsetCommitRequest => ConsumerGroupId ConsumerGroupGenerationId ConsumerId [TopicName [Partition Offset TimeStamp Metadata]]
     *   ConsumerGroupId => string
     *   ConsumerGroupGenerationId => int32
     *   ConsumerId => string
     *   TopicName => string
     *   Partition => int32
     *   Offset => int64
     *   TimeStamp => int64
     *   Metadata => string
     *  
     * v2 (supported in 0.9.0 or later)
     * OffsetCommitRequest => ConsumerGroup ConsumerGroupGenerationId ConsumerId RetentionTime [TopicName [Partition Offset Metadata]]
     *   ConsumerGroupId => string
     *   ConsumerGroupGenerationId => int32
     *   ConsumerId => string
     *   RetentionTime => int64
     *   TopicName => string
     *   Partition => int32
     *   Offset => int64
     *   Metadata => string
     * 
     * @param array $payloads
     * @access public
     * @return string
     */
    public function commitOffsetRequest($payloads)
    {
        if (!isset($payloads['data'])) {
            throw new \Kafka\Exception('given commit offset data invalid. `data` is undefined.');
        }

        if (!isset($payloads['group_id'])) {
            throw new \Kafka\Exception('given commit offset data invalid. `group_id` is undefined.');
        }

        $header = self::requestHeader('kafka-php', 0, self::OFFSET_COMMIT_REQUEST);
        $data   = self::encodeString($payloads['group_id'], self::PACK_INT16);
        $data  .= self::encodeArray($payloads['data'], array(__CLASS__, '_encodeCommitOffset'));
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $this->stream->write($data);
    }

    /**
     * Offset Fetch Request
     *
     * v0 and v1 (supported in 0.8.2 or after):
     * OffsetFetchRequest => ConsumerGroup [TopicName [Partition]]
     *   ConsumerGroup => string
     *   TopicName => string
     *   Partition => int32
     * @param array $payloads
     * @access public
     * @return string
     */
    public function fetchOffsetRequest($payloads)
    {
        if (!isset($payloads['data'])) {
            throw new \Kafka\Exception('given fetch offset data invalid. `data` is undefined.');
        }

        if (!isset($payloads['group_id'])) {
            throw new \Kafka\Exception('given fetch offset data invalid. `group_id` is undefined.');
        }

        $header = self::requestHeader('kafka-php', 0, self::OFFSET_FETCH_REQUEST);
        $data   = self::encodeString($payloads['group_id'], self::PACK_INT16);
        $data  .= self::encodeArray($payloads['data'], array(__CLASS__, '_encodeFetchOffset'));
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $this->stream->write($data);
    }

    /**
     * encode pack string type
     *
     * @param string $string
     * @param int $bytes   self::PACK_INT32: int32 big endian order. self::PACK_INT16: int16 big endian order.
     * @static
     * @access public
     * @return string
     */
    public static function encodeString($string, $bytes, $compression = self::COMPRESSION_NONE)
    {
        $packLen = ($bytes == self::PACK_INT32) ? self::BIT_B32 : self::BIT_B16;
        switch ($compression) {
            case self::COMPRESSION_NONE:
                break;
            case self::COMPRESSION_GZIP:
                $string = gzencode($string);
                break;
            case self::COMPRESSION_SNAPPY:
                throw new \Kafka\Exception('SNAPPY compression not yet implemented');
            default:
                throw new \Kafka\Exception('Unknown compression flag: ' . $compression);
        }
        return self::pack($packLen, strlen($string)) . $string;
    }

    /**
     * encode key array
     *
     * @param array $array
     * @param Callable $func
     * @static
     * @access public
     * @return string
     */
    public static function encodeArray(array $array, $func, $options = null)
    {
        if (!is_callable($func, false)) {
            throw new \Kafka\Exception('Encode array failed, given function is not callable.');
        }

        $arrayCount = count($array);

        $body = '';
        foreach ($array as $value) {
            if (!is_null($options)) {
                $body .= call_user_func($func, $value, $options);
            } else {
                $body .= call_user_func($func, $value);
            }
        }

        return self::pack(self::BIT_B32, $arrayCount) . $body;
    }

    /**
     * encode message set
     * N.B., MessageSets are not preceded by an int32 like other array elements
     * in the protocol.
     * MessageSet => [Offset MessageSize Message]
     *   Offset => int64
     *   MessageSize => int32
     * @param array $messages
     * @static
     * @access public
     * @return string
     */
    public static function encodeMessageSet($messages, $compression = self::COMPRESSION_NONE)
    {
        if (!is_array($messages)) {
            $messages = array($messages);
        }

        $data = '';
        foreach ($messages as $message) {
            $tmpMessage = self::_encodeMessage($message, $compression);

            // int64 -- message offset     Message
            $data .= self::pack(self::BIT_B64, 1) . self::encodeString($tmpMessage, self::PACK_INT32);
        }
        return $data;
    }

    /**
     * generate request header
     *
     * RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
     *   ApiKey => int16
     *   ApiVersion => int16
     *   CorrelationId => int32
     *   ClientId => string
     *   RequestMessage => MetadataRequest | ProduceRequest | FetchRequest | OffsetRequest | OffsetCommitRequest | OffsetFetchRequest
     *
     * @param string $clientId
     * @param integer $correlationId
     * @param integer $apiKey
     * @static
     * @access public
     * @return void
     */
    public static function requestHeader($clientId, $correlationId, $apiKey)
    {
        // int16 -- apiKey int16 -- apiVersion int32 correlationId
        $binData  = self::pack(self::BIT_B16, $apiKey);
        $binData .= self::pack(self::BIT_B16, self::API_VERSION);
        $binData .= self::pack(self::BIT_B32, $correlationId);

        // concat client id
        $binData .= self::encodeString($clientId, self::PACK_INT16);

        return $binData;
    }

    /**
     * encode message
     * MessageSet => [Offset MessageSize Message]
     *   Offset => int64
     *   MessageSize => int32
     * v1 (supported since 0.10.0)
     * Message => Crc MagicByte Attributes Key Value
     *   Crc => int32
     *   MagicByte => int8
     *   Attributes => int8
     *   Timestamp => int64
     *   Key => bytes
     *   Value => bytes
     * @param string $message
     * @static
     * @access protected
     * @return string
     */
    protected static function _encodeMessage($message, $compression = self::COMPRESSION_NONE)
    {
        // int8 -- magic  int8 -- attribute
        $data  = self::pack(self::BIT_B8, self::MESSAGE_MAGIC);
        $data .= self::pack(self::BIT_B8, $compression);
        //v0.10 new element timestamp
//        $data .= self::pack(self::BIT_B64, 1);
        // message key
        $data .= self::encodeString('', self::PACK_INT32);

        // message value
        $data .= self::encodeString($message, self::PACK_INT32, $compression);

        $crc = crc32($data);

        // int32 -- crc code  string data
        $message = self::pack(self::BIT_B32, $crc) . $data;
var_dump($message);
        return $message;
    }

    /**
     * encode signal part
     *
     * @param partions
     * @static
     * @access protected
     * @return string
     */
    protected static function _encodeProcudePartion($values, $compression)
    {
        if (!isset($values['partition_id'])) {
            throw new \Kafka\Exception('given produce data invalid. `partition_id` is undefined.');
        }

        if (!isset($values['messages']) || empty($values['messages'])) {
            throw new \Kafka\Exception('given produce data invalid. `messages` is undefined.');
        }

        $data = self::pack(self::BIT_B32, $values['partition_id']);
        $data .= self::encodeString(self::encodeMessageSet($values['messages'], $compression), self::PACK_INT32);

        return $data;
    }

    /**
     * encode signal topic
     *
     * @param partions
     * @static
     * @access protected
     * @return string
     */
    protected static function _encodeProduceTopic($values, $compression)
    {
        if (!isset($values['topic_name'])) {
            throw new \Kafka\Exception('given produce data invalid. `topic_name` is undefined.');
        }

        if (!isset($values['partitions']) || empty($values['partitions'])) {
            throw new \Kafka\Exception('given produce data invalid. `partitions` is undefined.');
        }

        $topic = self::encodeString($values['topic_name'], self::PACK_INT16);
        $partitions = self::encodeArray($values['partitions'], array(__CLASS__, '_encodeProcudePartion'), $compression);

        return $topic . $partitions;
    }

    /**
     * encode signal part
     *
     * @param partions
     * @static
     * @access protected
     * @return string
     */
    protected static function _encodeFetchPartion($values)
    {
        if (!isset($values['partition_id'])) {
            throw new \Kafka\Exception('given fetch data invalid. `partition_id` is undefined.');
        }

        if (!isset($values['offset'])) {
            $values['offset'] = 0;
        }

        if (!isset($values['max_bytes'])) {
            $values['max_bytes'] = 100 * 1024 * 1024;
        }

        $data = self::pack(self::BIT_B32, $values['partition_id']);
        $data .= self::pack(self::BIT_B64, $values['offset']);
        $data .= self::pack(self::BIT_B32, $values['max_bytes']);

        return $data;
    }

    /**
     * encode signal topic
     *
     * @param partions
     * @static
     * @access protected
     * @return string
     */
    protected static function _encodeFetchTopic($values)
    {
        if (!isset($values['topic_name'])) {
            throw new \Kafka\Exception('given fetch data invalid. `topic_name` is undefined.');
        }

        if (!isset($values['partitions']) || empty($values['partitions'])) {
            throw new \Kafka\Exception('given fetch data invalid. `partitions` is undefined.');
        }

        $topic = self::encodeString($values['topic_name'], self::PACK_INT16);
        $partitions = self::encodeArray($values['partitions'], array(__CLASS__, '_encodeFetchPartion'));

        return $topic . $partitions;
    }

    /**
     * encode signal part
     *
     * @param partions
     * @static
     * @access protected
     * @return string
     */
    protected static function _encodeOffsetPartion($values)
    {
        if (!isset($values['partition_id'])) {
            throw new \Kafka\Exception('given offset data invalid. `partition_id` is undefined.');
        }

        if (!isset($values['time'])) {
            $values['time'] = -1; // -1
        }

        if (!isset($values['max_offset'])) {
            $values['max_offset'] = 100000;
        }

        $data = self::pack(self::BIT_B32, $values['partition_id']);
        $data .= self::pack(self::BIT_B64, $values['time']);
        $data .= self::pack(self::BIT_B32, $values['max_offset']);

        return $data;
    }

    /**
     * encode signal topic
     *
     * @param partions
     * @static
     * @access protected
     * @return string
     */
    protected static function _encodeOffsetTopic($values)
    {
        if (!isset($values['topic_name'])) {
            throw new \Kafka\Exception('given offset data invalid. `topic_name` is undefined.');
        }

        if (!isset($values['partitions']) || empty($values['partitions'])) {
            throw new \Kafka\Exception('given offset data invalid. `partitions` is undefined.');
        }

        $topic = self::encodeString($values['topic_name'], self::PACK_INT16);
        $partitions = self::encodeArray($values['partitions'], array(__CLASS__, '_encodeOffsetPartion'));

        return $topic . $partitions;
    }

    /**
     * encode signal part
     *
     * @param partions
     * @static
     * @access protected
     * @return string
     */
    protected static function _encodeCommitOffsetPartion($values)
    {
        if (!isset($values['partition_id'])) {
            throw new \Kafka\Exception('given commit offset data invalid. `partition_id` is undefined.');
        }

        if (!isset($values['offset'])) {
            throw new \Kafka\Exception('given commit offset data invalid. `offset` is undefined.');
        }

        if (!isset($values['time'])) {
            $values['time'] = -1;
        }

        if (!isset($values['metadata'])) {
            $values['metadata'] = 'm';
        }

        $data = self::pack(self::BIT_B32, $values['partition_id']);
        $data .= self::pack(self::BIT_B64, $values['offset']);
$data .= self::pack(self::BIT_B64, $values['time']);
        $data .= self::encodeString($values['metadata'], self::PACK_INT16);

        return $data;
    }

    /**
     * encode signal topic
     *
     * @param partions
     * @static
     * @access protected
     * @return string
     */
    protected static function _encodeCommitOffset($values)
    {
        if (!isset($values['topic_name'])) {
            throw new \Kafka\Exception('given commit offset data invalid. `topic_name` is undefined.');
        }

        if (!isset($values['partitions']) || empty($values['partitions'])) {
            throw new \Kafka\Exception('given commit offset data invalid. `partitions` is undefined.');
        }

        $topic = self::encodeString($values['topic_name'], self::PACK_INT16);
        $partitions = self::encodeArray($values['partitions'], array(__CLASS__, '_encodeCommitOffsetPartion'));

        return $topic . $partitions;
    }

    /**
     * encode signal part
     *
     * @param partions
     * @static
     * @access protected
     * @return string
     */
    protected static function _encodeFetchOffsetPartion($values)
    {
        if (!isset($values['partition_id'])) {
            throw new \Kafka\Exception('given fetch offset data invalid. `partition_id` is undefined.');
        }

        $data = self::pack(self::BIT_B32, $values['partition_id']);

        return $data;
    }

    /**
     * encode signal topic
     *
     * @param partions
     * @static
     * @access protected
     * @return string
     */
    protected static function _encodeFetchOffset($values)
    {
        if (!isset($values['topic_name'])) {
            throw new \Kafka\Exception('given fetch offset data invalid. `topic_name` is undefined.');
        }

        if (!isset($values['partitions']) || empty($values['partitions'])) {
            throw new \Kafka\Exception('given fetch offset data invalid. `partitions` is undefined.');
        }

        $topic = self::encodeString($values['topic_name'], self::PACK_INT16);
        $partitions = self::encodeArray($values['partitions'], array(__CLASS__, '_encodeFetchOffsetPartion'));

        return $topic . $partitions;
    }
}
