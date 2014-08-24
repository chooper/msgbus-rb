#!/usr/bin/env ruby

require 'redis'
require 'retries'

class Channel

  # a wrapper around a redis client/connection object

  def initialize(redis_url, param={})
    @url = redis_url
    @timeout = param[:timeout] || 10 # seconds
  end

  def connect
    # ensures a probably-working connection to the given redis instance
    with_retries(:max_tries => 3) do
      @redis.ping unless @redis.nil?
      @redis = Redis.connect(url: @url) if @redis.nil? or not @redis.connected?
      @redis
    end
  end

  def pop(key)
    # pops an item from the head of the list with the given key
    loop do
      item = nil
      with_retries(:max_tries => 3) do
        self.connect
        # http://redis.io/commands/blpop
        item = @redis.blpop(key, @timeout)
      end
      return item unless item.nil?
    end
  end

  def push(key, message)
    # pushes an item onto the tail of a list with the given key
    with_retries(:max_tries => 3) do
      self.connect
      # http://redis.io/commands/rpush
      @redis.rpush(key, message)
    end
  end
end

