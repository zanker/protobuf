require 'thread'

module Protobuf
  module Rpc
    module Zmq
      class Broker
        include ::Protobuf::Rpc::Zmq::Util

        attr_reader :work_queue, :response_queue

        def initialize(server)
          @server = server

          init_zmq_context
          init_backend_socket
          init_frontend_socket
          init_poller

          @idle_workers = []
          @work_queue = ::Queue.new
          @response_queue = ::Queue.new
        rescue
          teardown
          raise
        end

        def run_frontend
          loop do
            rc = @frontend_poller.poll(5)

            # The server was shutdown and no requests are pending
            break if rc == 0 && !running?
            # Something went wrong
            break if rc == -1
            process_frontend if rc > 0

            response_queue.length.times do
              write_to_frontend(response_queue.shift)
            end
          end
        end

        def run
          Thread.new(self) do |broker|
            broker.run_frontend
          end

          loop do
            rc = @backend_poller.poll(5)

            # The server was shutdown and no requests are pending
            break if rc == 0 && !running?
            # Something went wrong
            break if rc == -1
            process_backend if rc > 0

            @idle_workers.each do |idle_worker|
              break if work_queue.empty?
              write_to_backend([idle_worker, ::Protobuf::Rpc::Zmq::EMPTY_STRING].concat(work_queue.shift))
            end
          end
        ensure
          teardown
        end

        def running?
          @server.running? || @server.workers.any?
        end

        private

        def init_backend_socket
          @backend_socket = @zmq_context.socket(ZMQ::ROUTER)
          zmq_error_check(@backend_socket.bind(@server.backend_uri))
        end

        def init_frontend_socket
          @frontend_socket = @zmq_context.socket(ZMQ::ROUTER)
          zmq_error_check(@frontend_socket.bind(@server.frontend_uri))
        end

        def init_poller
          @frontend_poller = ZMQ::Poller.new
          @frontend_poller.register_readable(@frontend_socket)

          @backend_poller = ZMQ::Poller.new
          @backend_poller.register_readable(@backend_socket)
        end

        def init_zmq_context
          if inproc?
            @zmq_context = @server.zmq_context
          else
            @zmq_context = ZMQ::Context.new
          end
        end

        def inproc?
          !!@server.try(:inproc?)
        end

        def process_backend
          worker, _, *frames = read_from_backend
          @idle_workers << worker

          unless frames == [::Protobuf::Rpc::Zmq::WORKER_READY_MESSAGE]
            response_queue << frames
          end
        end

        def process_frontend
          address, _, message, *frames = read_from_frontend

          if message == ::Protobuf::Rpc::Zmq::CHECK_AVAILABLE_MESSAGE
            if should_queue_request?
              write_to_frontend([address, ::Protobuf::Rpc::Zmq::EMPTY_STRING, ::Protobuf::Rpc::Zmq::WORKERS_AVAILABLE])
            else
              write_to_frontend([address, ::Protobuf::Rpc::Zmq::EMPTY_STRING, ::Protobuf::Rpc::Zmq::NO_WORKERS_AVAILABLE])
            end
          else
            work_queue << [address, ::Protobuf::Rpc::Zmq::EMPTY_STRING, message ].concat(frames)
          end
        end

        def read_from_backend
          frames = []
          zmq_error_check(@backend_socket.recv_strings(frames))
          frames
        end

        def read_from_frontend
          frames = []
          zmq_error_check(@frontend_socket.recv_strings(frames))
          frames
        end

        def should_queue_request?
          @idle_workers.size > 0 || work_queue.size < 10
        end

        def teardown
          @frontend_socket.try(:close)
          @backend_socket.try(:close)
          @zmq_context.try(:terminate) unless inproc?
        end

        def write_to_backend(frames)
          zmq_error_check(@backend_socket.send_strings(frames))
        end

        def write_to_frontend(frames)
          zmq_error_check(@frontend_socket.send_strings(frames))
        end
      end
    end
  end
end
