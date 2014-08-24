#!/usr/bin/env ruby

require 'json'
require 'securerandom'

class Message

  # a representation of a message. valid message `data` is in the form of
  # `[queue_name, {headers => {...}, payload => {...} }]`
  #
  # required headers:
  # * message_id (string): uuid for the message

  # TODO: implement expirations/ttls

  def initialize(queue, payload={}, headers={})
    @queue = queue.to_sym
    @payload = payload
    @headers = headers

    # generate message_id
    @headers["message_id"] = SecureRandom.uuid unless self.id
  end
    
  attr_reader :queue, :headers, :payload

  def id
    @headers["message_id"]
  end

  def self.from_json(data)
    data.force_encoding("utf-8") unless data.valid_encoding?
    data = JSON.parse(data)
    self.new(data[0], data[1]["payload"], data[1]["headers"])
  end

  def serialize
    content = [@queue.to_s, { "payload" => @payload, "headers" => @headers }]
    JSON.dump(content)
  end

  def to_s
    "<Message: #{self.serialize}>"
  end

  def ==(other)
    self.queue == other.queue and
      self.payload == other.payload and
      self.headers == other.headers
  end

  def valid?
    # TODO: check expiration
    return false if self.content.nil?
    return false if self.headers.nil?
    return false if self.payload.nil?
    return false if self.id.nil?
    true
  end
end

