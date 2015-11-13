<?php
namespace Odesk\Phystrix;

class MemcachedStateStorage implements StateStorageInterface
{
    const BUCKET_EXPIRE_SECONDS = 120;

    const CACHE_PREFIX = 'phystrix_cb_';

    const OPENED_NAME = 'opened';

    const SINGLE_TEST_BLOCKED = 'single_test_blocked';

    /**
     * @var \Memcached
     */
    private $memcached;

    public function __construct(\Memcached $memcached)
    {
        $this->memcached = $memcached;
    }

    /**
     * Prepends cache prefix and filters out invalid characters
     *
     * @param string $name
     * @return string
     */
    protected function prefix($name)
    {
        return self::CACHE_PREFIX . $name;
    }

    /**
     * Returns counter value for the given bucket
     *
     * @param string $commandKey
     * @param string $type
     * @param integer $index
     * @return integer
     */
    public function getBucket($commandKey, $type, $index)
    {
        $bucketName = $this->prefix($commandKey . '_' . $type . '_' . $index);
        return $this->memcached->get($bucketName);
    }

    /**
     * Increments counter value for the given bucket
     *
     * @param string $commandKey
     * @param string $type
     * @param integer $index
     */
    public function incrementBucket($commandKey, $type, $index)
    {
        $bucketName = $this->prefix($commandKey . '_' . $type . '_' . $index);
        $this->memcached->increment($bucketName, 1, 1, self::BUCKET_EXPIRE_SECONDS);
    }

    /**
     * If the given bucket is found, sets counter value to 0.
     *
     * @param string $commandKey Circuit breaker key
     * @param integer $type
     * @param integer $index
     */
    public function resetBucket($commandKey, $type, $index)
    {
        $bucketName = $this->prefix($commandKey . '_' . $type . '_' . $index);

        if ($this->memcached->get($bucketName)) {
            $this->memcached->set($bucketName, 0, self::BUCKET_EXPIRE_SECONDS);
        }
    }

    /**
     * Marks the given circuit  as open
     *
     * @param string $commandKey Circuit key
     * @param integer $sleepingWindowInMilliseconds In how much time we should allow a single test
     */
    public function openCircuit($commandKey, $sleepingWindowInMilliseconds)
    {
        $openedKey = $this->prefix($commandKey . self::OPENED_NAME);
        $singleTestFlagKey = $this->prefix($commandKey . self::SINGLE_TEST_BLOCKED);

        $this->memcached->set($openedKey, true);
        // the single test blocked flag will expire automatically in $sleepingWindowInMilliseconds
        // thus allowing us a single test. Notice, Memcache doesn't allow us to use
        // expire time less than a second.
        $sleepingWindowInSeconds = ceil($sleepingWindowInMilliseconds / 1000);
        $this->memcached->add($singleTestFlagKey, true, $sleepingWindowInSeconds);
    }

    /**
     * Whether a single test is allowed
     *
     * @param string $commandKey Circuit breaker key
     * @param integer $sleepingWindowInMilliseconds In how much time we should allow the next single test
     * @return boolean
     */
    public function allowSingleTest($commandKey, $sleepingWindowInMilliseconds)
    {
        $singleTestFlagKey = $this->prefix($commandKey . self::SINGLE_TEST_BLOCKED);
        $sleepingWindowInSeconds = ceil($sleepingWindowInMilliseconds / 1000);
        return (boolean) $this->memcached->add($singleTestFlagKey, true, $sleepingWindowInSeconds);
    }

    /**
     * Whether a circuit is open
     *
     * @param string $commandKey Circuit breaker key
     * @return boolean
     */
    public function isCircuitOpen($commandKey)
    {
        $openedKey = $this->prefix($commandKey . self::OPENED_NAME);
        return (boolean) $this->memcached->get($openedKey);
    }

    /**
     * Marks the given circuit as closed
     *
     * @param string $commandKey Circuit key
     */
    public function closeCircuit($commandKey)
    {
        $openedKey = $this->prefix($commandKey . self::OPENED_NAME);
        $this->memcached->set($openedKey, false);
    }
}
