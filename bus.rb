#!/usr/bin/env ruby

# spike: listen and publish messages across multiple channels, specifically for
# fault tolerance.
#
# ordering is not guaranteed.
#
# assumption #1: we know what channels exist ahead of time
# assumption #2: we'll just use redis lists for our channels

require 'thread'
require 'redis'
require 'retries'

class Channel
  def initialize(redis_url, key, param={})
    @url = redis_url
    @key = key
    @timeout = param[:timeout] || 10 # seconds
  end

  def connect
    with_retries(:max_tries => 3) do
      @redis.ping unless @redis.nil?
      @redis = Redis.connect(url: @url) if @redis.nil? or not @redis.connected?
      @redis
    end
  end

  def pop
    loop do
      item = nil
      with_retries(:max_tries => 3) do
        self.connect
        item = @redis.blpop(@key, @timeout)
      end
      return item unless item.nil?
    end
  end
end

class Bus
  def initialize(redis_urls, key, chan_params={})
    @urls = redis_urls
    @key = key

    @channels = []
    @urls.map { |u| @channels << Channel.new(u, key, chan_params) }
    @in_queue = Queue.new
  end

  def pop
    @in_queue.pop
  end

  def setup
    self.listen
  end

  def listen
    @channels.each do |c|
      # Start listeners
      Thread.new {
        loop do
          item = c.pop
          puts "bus -> in-queue: #{item}"
          @in_queue << item
        end
      }
    end
  end

  def join
    Thread.list.reject { |t| t == Thread.current or t == Thread.main }.each { |t| t.join }
  end
end

=begin
redis = ENV['REDIS_URLS'].split(',')[0]
c = Channel.new(redis, "testchan")
loop do
  puts "Got item: #{c.pop}"
end
=end

b = Bus.new(ENV['REDIS_URLS'].split(','), "testchan")
b.setup
b.join

