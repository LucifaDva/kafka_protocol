<?php
namespace Kafka;

/**
 * Kafka protocol since Kafka version 0.10
 * sockert class
 */
class Socket
{
    /**
     * read socket max lenght 10M.
     * 10*1024*1024B
     */
    const READ_MAX_LEN = 10485760;

    /**
     * send timeout in seconds.
     * @var float
     * @access private
     */
    private $sendTimeoutSec = 0;

    /**
     * send timeout in microseconds.
     * @var float
     * @access private
     */
    private $sendTimeoutUsec = 100000;

    /**
     * receive timeout in seconds.
     * @var float
     * @access private
     */
    private $recvTimeoutSec = 0;

    /**
     * receive timeout in microseconds.
     * @var float
     * @access private
     */
    private $recvTimeoutUsec = 750000;

    /**
     * Stream resource.
     * @var mixed
     * @access private
     */
    private $stream = null;

    /**
     * Socket host
     * @var mixed
     * @access private
     */
    private $host = null;

    /**
     * Socket host
     * @var mixed
     * @access private
     */        
    private $port = -1;

    /**
     *@access public
     *@return void
     */
    public function __construct($host, $port, $sendTimeoutSec = 5, $sendTimeoutUsec = 100000, $recvTimeoutSec = 5, $recvTimeoutUsec = 750000)
    {
        $this->host = $host;
        $this->port = $port;
        $this->sendTimeoutSec  = $sendTimeoutSec;
        $this->sendTimeoutUsec = $sendTimeoutUsec;
        $this->recvTimeoutSec  = $recvTimeoutSec;
        $this->recvTimeoutUsec = $recvTimeoutUsec;
    }

    /**
     * set the internal stream handle
     * @param mixed
     * @access public
     * @return void
     */
    public function setStream($stream)
    {
        $this->stream = $stream;
    }

    /**
     * set the internal stream handle
     * @param mixed
     * @access public
     * @return mixed $socket
     */
    public function createFromStream($stream)
    {
        $socket = new self("localhost", 0);
        $socket->setStream($stream);
        return $socket;
    }

    /**
     * Connect to the socket
     * @access public
     * @return bool
     */
    public function connect()
    {
        if (is_resource($this->stream)) {
            return  false;
        }
        if (empty($this->host)) {
            echo "cannot open null host\n";
            return false;
        }
        if ($this->port <= 0) {
            echo "cannot open null/negative port\n";
            return false;
        }

        $this->stream = @fsockopen($this->host, $this->port, $errno, $errstr, $this->sendTimeoutSec + ($this->sendTimeoutUsec / 1000000));
        if ($this->stream == false) {
            $error = "cannot connect to " . $this->host . ":" . $this->port . " (" . $errstr . " [" . $errno . "])\n";
            echo $error . "\n";
            return false;
        }
        //0-not blocking mode
        stream_set_blocking($this->stream, 0);
        return true;
    }

    /**
     * Close the socket
     * @access public
     * @return bool
     */
    public function close()
    {
        $ret = false;
        if(is_resource($this->stream)) {
            $ret = fclose($this->stream);
        }
        return $ret;
    }

    /**
     * read from the socket ,max $len bytes.
     * This method will not wait for all the requested data, it will return as
     * soon as any data is received.
     * @param integer $len
     * @param boolean if the length of read bytes less than $len
     * @return string data
     * @throws exception
     */
    public function read($len, $ifLessBygesThrow = false)
    {
        if ($len > self::READ_MAX_LEN) {
            $this->close();
            throw new Exception("cannot read " . $len . " bytes from stream, too long.");
        }
        $null = null;
        $read = array($this->stream);
        $readable = @stream_select($read, $null, $null,  $this->recvTimeoutSec, $this->recvTimeoutUsec);
        if ($readable > 0) {
            $readBytes = $len;
            $data = $chunk = "";
            while ($readBytes > 0) {
                $chunk = fread($this->stream, $readBytes);
                if ($chunk === false) {
                    $this->close();
                    throw new Exception("cannot read " . $len . " bytes from stream, no data.");
                }
                if (strlen($chunk) === 0) {
                    if (feof($this->stream)) {
                        $this->close();
                        throw new Exception("Unexpected EOF while reading " . $len . " bytes from stream, no data.");
                    }
                    $readable = @stream_select($read, $null, $null, $this->recvTimeoutSec, $this->recvTimeoutUsec);
                    if ($readable !== 1) {
                        $this->close();
                        throw new Exception("Timed out reading socket while reading " . $readBytes . " bytes to go.");
                    }
                    continue;
                }
                $data .= $chunk;
                $readBytes -= strlen($chunk);
            }
            if ($len == $readBytes || ($ifLessBygesThrow && $len !== strlen($data))) {
                $this->close();
                 throw new Exception("Read in fact " . strlen($data) . " bytes from stream, less than " . $len . ".");
            }
            return $data;
        }
        if (false !== $readable) {
            $res = stream_get_meta_data($this->stream);
            if (!empty($res['timed_out'])) {
                $this->close();
                throw new Exception("Timed out reading socket while reading " . $len . " bytes to go.");
            }
        }
        $this->close();
        throw new Exception("Could not read ".$len." bytes from stream (not readable)");
    }

    /**
     * write to the socket.
     *
     * @param string $buf The data to write
     * @return integer
     * @throws Kafka_Exception_Socket
     */
    public function write($buf)
    {
        $null = null;
        $write = array($this->stream);

        $hasWritenBytes = 0;
        $bufLen = strlen($buf);
        while ($hasWritenBytes < $bufLen) {
            $writeable = stream_select($null, $write, $null, $this->sendTimeoutSec, $this->sendTimeoutUsec);
            if ($writeable > 0) {
                $w = fwrite($this->stream, substr($buf, $hasWritenBytes));
                if ($w === -1 || $w === false) {
                    $this->close();
                    throw new Exception("cannot wiret " . $bufLen . " bytes, only write " . $hasWritenBytes . " bytes.");
                }
                $hasWritenBytes += $w;
                continue;
            }
            if (false !== $writeable) {
                $res = stream_get_meta_data($this->stream);
                if (!empty($res['timed_out'])) {
                    $this->close();
                    throw new Exception("Time out writing " . $bufLen . " to stream after writing " . $hasWritenBytes . " bytes.");
                }
            }
            $this->close();
            throw new Exception("Could not write ".$bufLen." bytes from stream (not writable)");
        }
        return $hasWritenBytes;
    }

    /**
     * @return boolean
     */
    public function rewind()
    {
        $ret = false;
        if (is_resource($this->stream)) {
            $ret = rewind($this->stream);
        }
        return $ret;
    }
}
