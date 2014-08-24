#!/usr/bin/env ruby

# spike: listen and publish messages across multiple channels, specifically for
# fault tolerance.
#
# ordering is not guaranteed.
#
# assumption #1: we know what channels exist ahead of time
# assumption #2: we'll just use redis lists for our channels

require 'thread'
require 'retries'
require './channel.rb'

Thread.abort_on_exception = true

class Bus
  def initialize(redis_urls, chan_params={})
    @urls = redis_urls

    @in_channels = []
    @in_queue = Queue.new

    @handlers = Hash.new { [] }
  end

  def pop
    @in_queue.pop
  end

  def setup
    self.start_listeners
    self.start_handlers
  end

  def start_listeners
    @handlers.each do |message_type, blk|
      @urls.each do |url|
        c = Channel.new(url)
        @in_channels << c
        Thread.new {
          loop do
            item = c.pop(message_type.to_s)
            puts "bus -> in-queue: #{item}"
            @in_queue << item
          end
        }
      end
    end
  end

  def join
    Thread.list.reject { |t| t == Thread.current or t == Thread.main }.each { |t| t.join }
  end

  def add_handler(message_type, &blk)
    @handlers[message_type] = Array.new unless @handlers.key?(message_type)
    @handlers[message_type] << blk
  end

  def start_handlers
    Thread.new {
      loop do
        type, message = self.pop
        puts "Finding handler for #{type}: #{message}"
        @handlers[type.to_sym].each do |blk|
          puts "Handler found: #{blk}"
          blk.call(message)
        end
      end
    }
  end
end

