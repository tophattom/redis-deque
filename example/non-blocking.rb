# frozen_string_literal: true

require 'redis-deque'

redis = Redis.new

queue = Redis::Deque.new('__test', redis: redis)
queue.clear true

100.times { queue << rand(100) }

queue.process(true) { |m| puts m }
