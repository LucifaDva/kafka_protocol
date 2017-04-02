<?php
namespace Kafka\Protocol;

/**
 * Kafka protocol since Kafka v0.10
 *
 * @package
 * @version 0.0.1
 * @copyright Copyleft
 * @author lucifa diva
*/

class Response extends Protocol
{
    private $data_;
    private $data_len_ = 0;
    private $offset_ = 0;

    private function _readInt8() {
        $data = substr($this->data_, $this->offset_, 1);
        $res = self::unpack(self::BIT_B8, $data);
        $res = array_shift($res);
        $this->offset_ += 1;
        return $res;
    }
    private function _readInt16() {
        $data = substr($this->data_, $this->offset_, 2);
        $res = self::unpack(self::BIT_B16, $data);
        $res = array_shift($res);
        $this->offset_ += 2;
        return $res;
    }
    private function _readInt32() {
        $data = substr($this->data_, $this->offset_, 4);
        $res = self::unpack(self::BIT_B32, $data);
        $res = array_shift($res);
        $this->offset_ += 4;
        return $res;
    }
    private function _readInt64() {
        $data = substr($this->data_, $this->offset_, 8);
        $res = self::unpack(self::BIT_B64, $data);
        $this->offset_ += 8;
        return $res;
    }
    private function _readString() {
        $len = $this->_readInt16();
        $data = substr($this->data_, $this->offset_, $len);
        $this->offset_ += $len;
        return $data;
    }
    private function _readBytes() {
        $len = $this->_readInt32();
        $data = substr($this->data_, $this->offset_, $len);
        $this->offset_ += $len;
        return $data;
    }

    /**
     * reseive data from Kafka Broker socket
     * @access private
     */
    private function _recvData() {
        $this->data_ = NULL;
        $this->offset_ = 0;
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        $dataLen = array_shift($dataLen);
        $this->data_ = $this->stream->read($dataLen, true);
        $this->data_len_ = $dataLen;
    }

    /**
     * Response => CorrelationId ResponseMessage
     * CorrelationId => int32
     * ResponseMessage => MetadataResponse | ProduceResponse | FetchResponse | OffsetResponse | OffsetCommitResponse | OffsetFetchResponse
     */

    /**
     * decode produce response
     *
     * v0
     * ProduceResponse => [TopicName [Partition ErrorCode Offset]]
     *   TopicName => string
     *   Partition => int32
     *   ErrorCode => int16
     *   Offset => int64
     *  
     * v1 (supported in 0.9.0 or later)
     * ProduceResponse => [TopicName [Partition ErrorCode Offset]] ThrottleTime
     *   TopicName => string
     *   Partition => int32
     *   ErrorCode => int16
     *   Offset => int64
     *   ThrottleTime => int32
     *  
     * v2 (supported in 0.10.0 or later)
     * ProduceResponse => [TopicName [Partition ErrorCode Offset Timestamp]] ThrottleTime
     *   TopicName => string
     *   Partition => int32
     *   ErrorCode => int16
     *   Offset => int64
     *   Timestamp => int64
     *   ThrottleTime => int32
     * @param string $data
     * @access public
     * @return array
     */
    public function produceResponse()
    {
        $result = array();
        $this->_recvData();
        $this->_readInt32();    //read CorrelationId
        $topicCount = $this->_readInt32();
        for ($i = 0; $i < $topicCount; $i++) {
            $topicName      = $this->_readString();
            $partitionCount = $this->_readInt32();
            $result[$topicName] = array();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionId = $this->_readInt32();
                $errorCode   = $this->_readInt16();
                $offset      = $this->_readInt64();
                //$timeStamp   = $this->_readInt64();
                $result[$topicName][$partitionId] = array(
                    'error_code' => $errorCode,
                    'offset'     => $offset,
                    'timestamp'  => $timeStamp,
                );
            }
        }
        return $result;
    }

    /**
     * Group Coordinator Response Protocol
     *
     * GroupCoordinatorResponse => ErrorCode CoordinatorId CoordinatorHost CoordinatorPort
     *   ErrorCode => int16
     *   CoordinatorId => int32
     *   CoordinatorHost => string
     *   CoordinatorPort => int32
     */
    public function consumerMetadataResponse() {
        $result = array();
        $this->_recvData();
        $this->_readInt32(); //read CorrelationId
        $errorCode       = $this->_readInt16();
        $coordinatorId   = $this->_readInt32();
        $coordinatorHost = $this->_readString();
        $coordinatorPort = $this->_readInt32();

        $result = array(
            'error_code'        => $errorCode,
            'coordinator_id'    => $coordinatorId,
            'coordinator_host'  => $coordinatorHost,
            'coordinator_port'  => $coordinatorPort,
        );
        return $result;
    }

    /**
     * Fetch Response Protocol
     *
     * v0
     * FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
     *   TopicName => string
     *   Partition => int32
     *   ErrorCode => int16
     *   HighwaterMarkOffset => int64
     *   MessageSetSize => int32
     * 
     * v1 (supported in 0.9.0 or later) and v2 (supported in 0.10.0 or later)
     * FetchResponse => ThrottleTime [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
     *   ThrottleTime => int32
     *   TopicName => string
     *   Partition => int32
     *   ErrorCode => int16
     *   HighwaterMarkOffset => int64
     *   MessageSetSize => int32
     * @param string $data
     * @access public
     * @return Iterator
     */
    public function fetchResponse()
    {
        $result = array();
        $this->_recvData();
        $this->_readInt32(); //read CorrelationId
//        $this->_readInt32(); //read ThrottleTime
        $topicCount = $this->_readInt32();
        for ($i = 0; $i < $topicCount; $i++) {
            $topicName = $this->_readString();
            $result[$topicName] = array();
            $partitionCount = $this->_readInt32();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionId = $this->_readInt32();
                $errorCode   = $this->_readInt16();
                $hightWater  = $this->_readInt64();
                $messageSetSize = $this->_readInt32();
                $result[$topicName][$partitionId] = array(
                    'error_code'       => $errorCode,
                    'high_water'       => $hightWater,
                    'message_set_size' => $messageSetSize,
                    'message_set'      => $this->_resolveMessageSet($messageSetSize, $partitionId),
                );
            }
        }
        return $result;
    }

    /**
     * resolve message set from stream
     * MessageSet => [Offset MessageSize Message]
     *   Offset => int64
     *   MessageSize => int32
     * 
     * @access private
     */
    private function _resolveMessageSet($messagetSetSize, $partitionId) {
	$message_set = array();
        //print "into resolveMessageSet\n";
        for ($i = 0; ; $i++) {
            if ($this->offset_ + 8 + 4 + 4 + 1 + 1 + 8 > $this->data_len_) {
                break;
            }
            $offset = $this->_readInt64();
            $message_size = $this->_readInt32();
            if ($message_size < 0) {
                \kafka\Log::write(\Kafka\Log::ERROR, "message_size:$message_size");
            }
            if ($this->offset_ + $message_size > $this->data_len_) {
                break;
            }
            $message = $this->_resolveMessage();
            $message['offset'] = $offset;
            $message['partition'] = $partition;
            $message_set[$i] = $message;
        }
        return $message_set;
    }

    /**
     * resolve message
     * 
     * v0
     * Message => Crc MagicByte Attributes Key Value
     *   Crc => int32
     *   MagicByte => int8
     *   Attributes => int8
     *   Key => bytes
     *   Value => bytes
     *  
     * v1 (supported since 0.10.0)
     * Message => Crc MagicByte Attributes Key Value
     *   Crc => int32
     *   MagicByte => int8
     *   Attributes => int8
     *   Timestamp => int64
     *   Key => bytes
     *   Value => bytes
     * @access private
     */
    private function _resolveMessage() {
        $result = array();
        $crc = $this->_readInt32();
        $magic = $this->_readInt8();
        $attributes = $this->_readInt8();
        //$timestamp = $this->_readInt64();
        if ($attributes != 0) {
            \kafka\Log::write(\Kafka\Log::ERROR,"compression:$attributes is not support now, exit");
            exit;
        }
        $key = $this->_readBytes();

        $value = $this->_readBytes();
        $result['crc'] = $crc;
        $result['magic'] = $magic;
        $result['attributes'] = $attributes;
        $result['key'] = $key;
        $result['value'] = $value;
        return $result;
    }

    /**
     * Metadata Response Protocol
     * MetadataResponse => [Broker][TopicMetadata]
     *   Broker => NodeId Host Port  (any number of brokers may be returned)
     *     NodeId => int32
     *     Host => string
     *     Port => int32
     *   TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
     *     TopicErrorCode => int16
     *   PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
     *     PartitionErrorCode => int16
     *     PartitionId => int32
     *     Leader => int32
     *     Replicas => [int32]
     *     Isr => [int32]
     * @param string $data
     * @access public
     * @return array
     */
    public function metadataResponse()
    {
        $result = array();
        $broker = array();
        $topic = array();
        $this->_recvData();
        $this->_readInt32(); //read CorrelationId
        $brokerCount = $this->_readInt32();
        for ($i = 0; $i < $brokerCount; $i++) {
            $nodeId   = $this->_readInt32();
            $hostName = $this->_readString();
            $port     = $this->_readInt32();
            $broker[$nodeId] = array(
                'host' => $hostName,
                'port' => $port,
            );
        }

        $topicMetaCount = $this->_readInt32();
        for ($i = 0; $i < $topicMetaCount; $i++) {
            $topicErrCode   = $this->_readInt16();
            $topicName      = $this->_readString();
            $partitionCount = $this->_readInt32();
            $topic[$topicName]['errCode'] = $topicErrCode;
            $partitions = array();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionErrCode = $this->_readInt16();
                $partitionId = $this->_readInt32();
                $leaderId    = $this->_readInt32();
                $repliasCount = $this->_readInt32();
                $replias = array();
                for ($k = 0; $k < $repliasCount; $k++) {
                    $repliaId = $this->_readInt32();
                    $replias[] = $repliaId;
                }
                $isrCount = $this->_readInt32();
                $isrs = array();
                for ($k = 0; $k < $isrCount; $k++) {
                    $isrId = $this->_readInt32();
                    $isrs[] = $isrId;
                }
                $partitions[$partitionId] = array(
                    'error_code'  => $partitionErrCode,
                    'leader'   => $leaderId,
                    'replicas' => $replias,
                    'isr'      => $isrs,
                );
            }
            $topic[$topicName]['partitions'] = $partitions;
        }

        $result = array(
            'brokers' => $broker,
            'topics'  => $topic,
        );
        return $result;
    }

    /**
     * decode offset response
     *
     * v0
     * OffsetResponse => [TopicName [PartitionOffsets]]
     *   PartitionOffsets => Partition ErrorCode [Offset]
     *   Partition => int32
     *   ErrorCode => int16
     *   Offset => int64
     * v1
     * ListOffsetResponse => [TopicName [PartitionOffsets]]
     *   PartitionOffsets => Partition ErrorCode Timestamp [Offset]
     *   Partition => int32
     *   ErrorCode => int16
     *   Timestamp => int64
     *   Offset => int64
     * @param string $data
     * @access public
     * @return array
     */


    public function offsetResponse()
    {
        $result = array();
        $this->_recvData();
	    $this->_readInt32(); //read CorrelationId
        $topicCount = $this->_readInt32();
        for ($i = 0; $i < $topicCount; $i++) {
            $topicName = $this->_readString();
            $partitionCount = $this->_readInt32();
            $result[$topicName] = array();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionId = $this->_readInt32();
                $errorCode   = $this->_readInt16();
                //$timestamp = $this->_readInt64();
                $offsetCount = $this->_readInt32();;
                $offsetArr = array();
                for ($k = 0; $k < $offsetCount; $k++) {
                    $offsetArr[] = $this->_readInt64();
                }
                $result[$topicName][$partitionId] = array(
                    'error_code' => $errorCode,
                    'offset'     => $offsetArr
                );
            }
        }
        return $result;
    }

    /**
     * Offset Commit Response Protocol
     *
     * v0, v1 and v2:
     * OffsetCommitResponse => [TopicName [Partition ErrorCode]]]
     *   TopicName => string
     *   Partition => int32
     *   ErrorCode => int16
     * @param string $data
     * @access public
     * @return array
     */
    public function commitOffsetResponse()
    {
        $result = array();
        $this->_recvData();
        $this->_readInt32(); //read CorrelationId
        $topicCount = $this->_readInt32();
        for ($i = 0; $i < $topicCount; $i++) {
            $topicName      = $this->_readString();
            $partitionCount = $this->_readInt32();
            $result[$topicName] = array();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionId = $this->_readInt32();
                $errCode     = $this->_readInt16();
                $result[$topicName][$partitionId] = array(
                    'error_code' => $errCode,
                );
            }
        }
        return $result;
    }


    /**
     * Fetch Offset Response Protocol
     *
     * v0 and v1 (supported in 0.8.2 or after):
     * OffsetFetchResponse => [TopicName [Partition Offset Metadata ErrorCode]]
     *   TopicName => string
     *   Partition => int32
     *   Offset => int64
     *   Metadata => string
     *   ErrorCode => int16
     * @param string $data
     * @access public
     * @return array
     */
    public function fetchOffsetResponse()
    {        
        $result = array();
        $this->_recvData();
        $this->_readInt32(); //read CorrelationId
        $topicCount = $this->_readInt32();
        for ($i = 0; $i < $topicCount; $i++) {
            $topicName      = $this->_readString();
            $partitionCount = $this->_readInt32();
            $result[$topicName] = array();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionId     = $this->_readInt32();
                $partitionOffset = $this->_readInt64();
                $metaData        = $this->_readString();
                $errorCode       = $this->_readInt16();
                $result[$topicName][$partitionId] = array(
                    'offset'      => $partitionOffset,
                    'metadata'    => $metaData,
                    'error_code'  => $errorCode,
                );
            }
        }
        return $result;
    }

    /**
     * get error
     *
     * @param integer $errCode
     * @static
     * @access public
     * @return string
     */
    public static function getError($errCode)
    {
        $error = '';
        switch($errCode) {
            case 0:
                $error = 'No error--it worked!';
                break;
            case -1:
                $error = 'An unexpected server error';
                break;
            case 1:
                $error = 'The requested offset is outside the range of offsets maintained by the server for the given topic/partition.';
                break;
            case 2:
                $error = 'This indicates that a message contents does not match its CRC';
                break;
            case 3:
                $error = 'This request is for a topic or partition that does not exist on this broker.';
                break;
            case 4:
                $error = 'The message has a negative size';
                break;
            case 5:
                $error = 'This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes';
                break;
            case 6:
                $error = 'This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.';
                break;
            case 7:
                $error = 'This error is thrown if the request exceeds the user-specified time limit in the request.';
                break;
            case 8:
                $error = 'This is not a client facing error and is used only internally by intra-cluster broker communication.';
                break;
            case 10:
                $error = 'The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.';
                break;
            case 11:
                $error = 'Internal error code for broker-to-broker communication.';
                break;
            case 12:
                $error = 'If you specify a string larger than configured maximum for offset metadata';
                break;
            case 14:
                $error = 'The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).';
                break;
            case 15:
                $error = 'The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.';
                break;
            case 16:
                $error = 'The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.';
                break;
            default:
                $error = 'Unknown error';
        }

        return $error;
    }
}
