#!/usr/bin/env ruby

# listen and publish messages across multiple redis instances

require 'thread'
require 'retries'
require './channel.rb'

# without this, threads will swallow exceptions and die silently
Thread.abort_on_exception = true

class Bus

  # allows connecting to multiple redises and pushing and popping
  # messages from them

  def initialize(redis_urls, chan_params={})
    @urls = redis_urls

    # the in_queue is used to hold inbound messages popped off from
    # the redis instances
    @in_channels = []
    @in_queue = Queue.new

    # the out-queue is used to hold outbound messages that are waiting
    # to be pushed into redis
    @out_channels = []
    @out_queue = Queue.new

    # maps inbound message_types -> Procs
    @handlers = Hash.new { [] }
  end

  def pop
    # pop a message off the local in-queue (hence off of the redis bus)
    @in_queue.pop
  end

  def push(message_type, message)
    # push a message into the local out-queue (hence on to the redis bus)
    @out_queue.push([message_type.to_s, message])
  end

  def setup
    # convenience function that sets up all of the Threads and their Channels
    self.start_publishers
    self.start_listeners
    self.start_handlers
  end

  def start_publishers
    # set up one Thread per redis for publishing messages.
    # the thread will pop messages off of the local out-queue and push them
    # onto one of the redis instances
    @urls.each do |url|
      c = Channel.new(url)
      @out_channels << c
      Thread.new {
        loop do
          message_type, message = @out_queue.pop
          puts "out-queue -> bus: #{[message_type.to_s, message]}"
          c.push(message_type, message)
        end
      }
    end
  end

  def start_listeners
    # set up a Thread per (message_type, redis) pair for receiving messages.
    # the threads will pop messages off of the various redis instances and push
    # them into the local in-queue
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
    # wait for all child threads (block until they're all dead)
    Thread.list.reject { |t| t == Thread.current or t == Thread.main }.each { |t| t.join }
  end

  def add_handler(message_type, &blk)
    # register the Proc blk with the given message_type
    @handlers[message_type] = Array.new unless @handlers.key?(message_type)
    @handlers[message_type] << blk
  end

  def start_handlers
    # start a Thread for handling received messages.
    # the thread pops messages off the local in-queue and runs the Proc(s)
    # registered for their message type. the blocks are passed the message
    # itself
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

