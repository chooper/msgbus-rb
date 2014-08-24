#!/usr/bin/env ruby

require 'redis'
require 'retries'

class Channel
  def initialize(redis_url, param={})
    @url = redis_url
    @timeout = param[:timeout] || 10 # seconds
  end

  def connect
    with_retries(:max_tries => 3) do
      @redis.ping unless @redis.nil?
      @redis = Redis.connect(url: @url) if @redis.nil? or not @redis.connected?
      @redis
    end
  end

  def pop(key)
    loop do
      item = nil
      with_retries(:max_tries => 3) do
        self.connect
        item = @redis.blpop(key, @timeout)
      end
      return item unless item.nil?
    end
  end
end

