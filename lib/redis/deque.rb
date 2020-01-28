# frozen_string_literal: true

class Redis
  class Deque
    VERSION = '0.1.0'

    def self.version
      "redis-deque version #{VERSION}"
    end

    def initialize(queue_name, options = {})
      raise ArgumentError, 'queue_name must be a non-empty string'  if !queue_name.is_a?(String) || queue_name.empty?
      raise ArgumentError, 'process_queue_name must be a non-empty string' if options.key?(:process_queue_name) && (!options[:process_queue_name].is_a?(String) || options[:process_queue_name].empty?)
      raise ArgumentError, 'queue_name and process_queue_name must be different' if options[:process_queue_name] == queue_name

      @redis = options[:redis] || Redis.current
      @queue_name = queue_name
      @process_queue_name = options[:process_queue_name] || "#{queue_name}_process"
      @last_message = nil
      @timeout = options[:timeout] ||= 0
    end

    def length
      @redis.llen @queue_name
    end

    def clear(clear_process_queue = false)
      @redis.del @queue_name
      @redis.del @process_queue_name if clear_process_queue
    end

    def empty?
      length <= 0
    end

    def push(obj)
      @redis.lpush(@queue_name, obj)
    end

    def unshift(obj)
      @redis.rpush(@queue_name, obj)
    end

    def pop(non_block = false)
      @last_message = if non_block
                        @redis.rpoplpush(@queue_name, @process_queue_name)
                      else
                        @redis.brpoplpush(@queue_name, @process_queue_name, @timeout)
                      end
      @last_message
    end

    def commit
      @redis.lrem(@process_queue_name, 0, @last_message)
    end

    def process(non_block = false, timeout = nil)
      @timeout = timeout unless timeout.nil?
      loop do
        message = pop(non_block)
        ret = yield message if block_given?
        commit if ret
        break if message.nil? || (non_block && empty?)
      end
    end

    def refill
      while (message = @redis.lpop(@process_queue_name))
        unshift(message)
      end
      true
    end

    alias size  length
    alias dec   pop
    alias shift pop
    alias enc   push
    alias <<    push
  end
end
