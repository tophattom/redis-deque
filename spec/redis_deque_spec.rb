# frozen_string_literal: true

require 'spec_helper'
require 'timeout'

describe Redis::Deque do
  before(:all) do
    @redis = Redis.new
    @queue = Redis::Deque.new('__test', process_queue_name: 'bp__test')
    @queue.clear true
  end

  before(:each) do
    @queue.clear true
  end

  after(:all) do
    @queue.clear true
  end

  after(:each) do
    @queue.clear true
  end

  it 'should return correct version string' do
    Redis::Deque.version.should == "redis-deque version #{Redis::Deque::VERSION}"
  end

  it 'should create a new redis-queue object' do
    queue = Redis::Deque.new('__test', process_queue_name: 'bp__test')
    queue.class.should == Redis::Deque
  end

  it 'should create default process_queue_name if one is not given' do
    queue = Redis::Deque.new '__test_queue'
    queue.instance_variable_get(:@process_queue_name).should be == '__test_queue_process'
  end

  it 'should not allow same name for queue_name and process_queue_name' do
    expect {
      Redis::Deque.new '__test_queue', process_queue_name: '__test_queue'
    }.to raise_error(ArgumentError)
  end

  it 'should add an element to the queue' do
    @queue << 'a'
    @queue.size.should be == 1
  end

  it 'should add an element to the front of the queue' do
    @queue.unshift 'a'
    @queue.unshift 'b'
    @queue.size.should be == 2

    message = @queue.pop(true)
    message.should be == 'b'
  end

  it 'should return an element from the queue' do
    @queue << 'a'
    message = @queue.pop(true)
    message.should be == 'a'
  end

  it 'should remove the last element from bp_queue if commit_last is called' do
    @queue << 'a'
    @queue << 'b'
    @queue.pop true
    @queue.pop true

    @redis.llen('bp__test').should be == 2
    @queue.commit_last
    @redis.llen('bp__test').should be == 1
    @redis.lpop('bp__test').should be == 'a'
  end

  it 'should remove specific element from bp_queue if commit is called' do
    @queue << 'a'
    @queue << 'b'
    @queue.pop true
    @queue.pop true

    @redis.llen('bp__test').should be == 2
    @queue.commit 'a'
    @redis.llen('bp__test').should be == 1
    @redis.lpop('bp__test').should be == 'b'
  end

  it 'should remove everything from bp_queue if commit_all is called' do
    @queue << 'a'
    @queue << 'b'
    @queue.pop true
    @queue.pop true

    @redis.llen('bp__test').should be == 2
    @queue.commit_all
    @redis.llen('bp__test').should be == 0
  end

  it 'should implements fifo pattern' do
    @queue.clear
    payload = %w[a b c d e]
    payload.each { |e| @queue << e }
    test = []
    while (e = @queue.pop(true))
      test << e
    end
    payload.should be == test
  end

  it 'should implement lifo pattern with unshift' do
    payload = %w(a b c d e)
    payload.each { |e| @queue.unshift e }
    test = []
    while (e = @queue.pop(true))
      test << e
    end
    test.should be == payload.reverse
  end

  it 'should remove all of the elements from the main queue' do
    %w[a b c d e].each { |e| @queue << e }
    @queue.size.should be > 0
    @queue.pop(true)
    @queue.clear
    @redis.llen('bp__test').should be > 0
  end

  it 'should reset queues content' do
    @queue.clear(true)
    @redis.llen('bp__test').should be == 0
  end

  it 'should prcess a message' do
    @queue << 'a'
    @queue.process(true) { |m| m.should be == 'a'; true }
  end

  it 'should prcess a message leaving it into the bp_queue' do
    @queue << 'a'
    @queue << 'a'
    @queue.process(true) { |m| m.should be == 'a'; false }
    @redis.lrange('bp__test', 0, -1).should be == %w[a a]
  end

  it 'should refill a main queue' do
    @queue.clear(true)
    @queue << 'a'
    @queue << 'a'
    @queue.process(true) { |m| m.should be == 'a'; false }
    @redis.lrange('bp__test', 0, -1).should be == %w[a a]
    @queue.refill
    @redis.lrange('__test', 0, -1).should be == %w[a a]
    @redis.llen('bp__test').should be == 0
  end

  it 'should work with the timeout parameters' do
    @queue.clear(true)
    2.times { @queue << rand(100) }
    is_ok = true
    begin
      Timeout.timeout(3) do
        @queue.process(false, 2) { |_m| true }
      end
    rescue Timeout::Error => _e
      is_ok = false
    end

    is_ok.should be_truthy
  end

  it 'should honor the timeout param in the initializer' do
    redis = Redis.new
    queue = Redis::Deque.new('__test_tm', process_queue_name: 'bp__test_tm', redis: redis, timeout: 2)
    queue.clear true

    is_ok = true
    begin
      Timeout.timeout(4) do
        queue.pop
      end
    rescue Timeout::Error => _e
      is_ok = false
    end
    queue.clear
    is_ok.should be_truthy
  end
end
