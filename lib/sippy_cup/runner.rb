# encoding: utf-8
require 'logger'
require 'fileutils'

#
# Service object to oversee the execution of a Scenario
#
module SippyCup
  class Runner
    attr_accessor :sipp_pid

    #
    # Create a runner from a scenario
    #
    # @param [Scenario, XMLScenario] scenario The scenario to execute
    # @param [Hash] opts Options to modify the runner
    # @option opts [optional, true, false] :full_sipp_output Whether or not to copy SIPp's stdout/stderr to the parent process. Defaults to true.
    # @option opts [optional, true, false] :sudo Whether or not to invoke SIPp with sudo. Defaults to true.
    # @option opts [optional, Logger] :logger A logger to use in place of the internal logger to STDOUT.
    # @option opts [optional, String] :command The command to execute. This is mostly available for testing.
    #
    def initialize(scenario, opts = {})
      @scenario = scenario
      @scenario_options = @scenario.scenario_options

      defaults = { full_sipp_output: true, sudo: true }
      @options = defaults.merge(opts)

      @command = @options[:command]
      @logger = @options[:logger] || Logger.new(STDOUT)
    end

    #
    # Runs the loaded scenario using SIPp
    #
    def run
      @input_files = @scenario.to_tmpfiles

      @logger.info "Preparing to run SIPp command: #{command}"
      @logger.info "SIPp scenario options: #{@scenario_options.inspect}" if @logger.respond_to?(:debug)

      execute_with_redirected_streams

      wait unless @options[:async]
    ensure
      cleanup_input_files unless @options[:async]
    end

    #
    # Tries to stop SIPp by killing the target PID
    #
    # @raises Errno::ESRCH when the PID does not correspond to a known process
    # @raises Errno::EPERM when the process referenced by the PID cannot be killed
    #
    def stop
      Process.kill "KILL", @sipp_pid if @sipp_pid
    end

    #
    # Waits for the runner to finish execution
    #
    # @raises Errno::ENOENT when the SIPp executable cannot be found
    # @raises SippyCup::ExitOnInternalCommand when SIPp exits on an internal command. Calls may have been processed
    # @raises SippyCup::NoCallsProcessed when SIPp exit normally, but has processed no calls
    # @raises SippyCup::FatalError when SIPp encounters a fatal failure
    # @raises SippyCup::FatalSocketBindingError when SIPp fails to bind to the specified socket
    # @raises SippyCup::SippGenericError when SIPp encounters another type of error
    #
    # @return Boolean true if execution succeeded without any failed calls, false otherwise
    #
    def wait
      exit_status = Process.wait2 @sipp_pid.to_i

      # Wait for threads to finish reading before closing streams
      @stderr_thread.join if @stderr_thread
      @stdout_thread.join if @stdout_thread

      @err_rd.close if @err_rd
      @stdout_rd.close if @stdout_rd

      # Log stderr output for debugging (only if logger responds to debug)
      if @stderr_buffer && !@stderr_buffer.empty? && @logger.respond_to?(:debug)
        @logger.debug "SIPp stderr output: #{@stderr_buffer}"
      end

      # Analyze output for communication issues
      analyze_sipp_output

      final_result = process_exit_status exit_status, @stderr_buffer
      if final_result
        @logger.info "Test completed successfully!"
      else
        @logger.info "Test completed successfully but some calls failed."
      end
      @logger.info "Statistics logged at #{File.expand_path @scenario_options[:stats_file]}" if @scenario_options[:stats_file]

      final_result
    ensure
      cleanup_input_files
    end

  private

    def command
      @command ||= begin
        command = @options[:sudo] ? "sudo $(which sipp)" : 'sipp'
        command_options.each_pair do |key, value|
          command << (value ? " -#{key} #{value}" : " -#{key}")
        end
        command << " #{@scenario_options[:destination]}"
      end
    end

    def command_options
      options = {
        p: @scenario_options[:source_port] || '8836',
        sf: @input_files[:scenario].path,
      }

      max_concurrent = @scenario_options[:concurrent_max] || @scenario_options[:max_concurrent]
      options[:l] = max_concurrent if max_concurrent
      options[:m] = @scenario_options[:number_of_calls] if @scenario_options[:number_of_calls]
      options[:r] = @scenario_options[:calls_per_second] if @scenario_options[:calls_per_second]
      options[:s] = @scenario_options[:to].to_s.split('@').first if @scenario_options[:to]

      options[:i] = @scenario_options[:source] if @scenario_options[:source]
      options[:mp] = @scenario_options[:media_port] if @scenario_options[:media_port]

      if @scenario_options[:calls_per_second_max]
        options[:no_rate_quit] = nil
        options[:rate_max] = @scenario_options[:calls_per_second_max]
        options[:rate_increase] = @scenario_options[:calls_per_second_incr] || 1
        options[:rate_interval] = @scenario_options[:calls_per_second_interval] if @scenario_options[:calls_per_second_interval]
      end

      if @scenario_options[:stats_file]
        options[:trace_stat] = nil
        options[:stf] = @scenario_options[:stats_file]
        options[:fd] = @scenario_options[:stats_interval] || 1
      end

      if @scenario_options[:summary_report_file]
        options[:trace_screen] = nil
        options[:screen_file] = @scenario_options[:summary_report_file]
      end

      if @scenario_options[:errors_report_file]
        options[:trace_err] = nil
        options[:error_file] = @scenario_options[:errors_report_file]
      end

      if @scenario_options[:transport_mode]
        options[:t] = @scenario_options[:transport_mode]
      end

      if @scenario_options[:scenario_variables]
        options[:inf] = @scenario_options[:scenario_variables]
      end

      options.merge! @scenario_options[:options] if @scenario_options[:options]

      options
    end

    def execute_with_redirected_streams
      @err_rd, err_wr = IO.pipe
      stdout_target = if @options[:full_sipp_output]
        @stdout_rd, stdout_wr = IO.pipe
        stdout_wr
      else
        '/dev/null'
      end

      @sipp_pid = spawn command, err: err_wr, out: stdout_target

      @stderr_buffer = String.new

      @stderr_thread = Thread.new do
        err_wr.close
        begin
          until @err_rd.eof?
            buffer = @err_rd.readpartial(1024).strip
            @stderr_buffer += buffer
            $stderr << buffer if @options[:full_sipp_output]
          end
        rescue IOError
          # Stream was closed, thread can exit
        end
      end

      if @stdout_rd
        @stdout_buffer = String.new

        @stdout_thread = Thread.new do
          stdout_wr.close
          begin
            until @stdout_rd.eof?
              buffer = @stdout_rd.readpartial(1024).strip
              @stdout_buffer += buffer
              $stdout << buffer
            end
          rescue IOError
            # Stream was closed, thread can exit
          end
        end
      end
    end

    def process_exit_status(process_status, error_message = nil)
      exit_code = process_status[1].exitstatus
      case exit_code
      when 0
        true
      when 1
        false
      when 97
        raise SippyCup::ExitOnInternalCommand, error_message
      when 99
        raise SippyCup::NoCallsProcessed, error_message
      when 255, -1
        raise SippyCup::FatalError, error_message
      when 254, -2
        raise SippyCup::FatalSocketBindingError, error_message
      when 2
        # SIPp exit code 2: error resolving hostname or connection refused
        enhanced_message = "SIPp failed to connect to target (exit code 2). Check hostname resolution and target availability. #{error_message}"
        raise SippyCup::SippGenericError, enhanced_message
      when 3
        # SIPp exit code 3: target not responding
        enhanced_message = "SIPp target not responding (exit code 3). Check if target service is running and accessible. #{error_message}"
        raise SippyCup::SippGenericError, enhanced_message
      else
        # Keep backwards compatibility - only enhance for specific networking issues
        raise SippyCup::SippGenericError, error_message
      end
    end

    def analyze_sipp_output
      return unless @logger.respond_to?(:warn) && @logger.respond_to?(:info)

      stderr_output = @stderr_buffer || ""
      stdout_output = @stdout_buffer || ""
      combined_output = stderr_output + stdout_output

      # Check for common SIP communication issues
      if combined_output.include?("Resolving remote host") && combined_output.include?("Done")
        @logger.info "SIPp successfully resolved hostname"
      end

      if combined_output.include?("No message received") || combined_output.include?("timeout")
        @logger.warn "SIPp indicates no response from SIP server - check if server is listening and accessible"
      end

      if combined_output.include?("Connection refused") || combined_output.include?("No route to host")
        @logger.warn "SIPp cannot connect to target - check network connectivity and firewall rules"
      end

      if combined_output.include?("Unexpected message received")
        @logger.warn "SIPp received unexpected SIP message - possible protocol mismatch"
      end

      # Look for call statistics - check multiple patterns
      calls_processed = 0
      if combined_output =~ /(\d+)\s+calls?\s+processed/i
        calls_processed = $1.to_i
      elsif combined_output =~ /Total-time\s+\|\s+(\d+)/
        calls_processed = $1.to_i
      end

      if calls_processed == 0
        @logger.warn "SIPp processed 0 calls - no SIP communication occurred"
        @logger.info "Possible causes: SIP server not running, wrong port, firewall blocking, or protocol mismatch"
      else
        @logger.info "SIPp processed #{calls_processed} call(s)"
      end

      # Check if SIPp indicates it's waiting
      if combined_output.include?("Waiting") || combined_output.include?("Paused")
        @logger.info "SIPp is waiting for server response"
      end

      # Debug: Log portions of output if debug logging is available
      if @logger.respond_to?(:debug) && !combined_output.empty?
        @logger.debug "SIPp combined output (first 500 chars): #{combined_output[0..500]}"
      end
    end

    def cleanup_input_files
      @input_files.values.compact.each do |value|
        value.close
        value.unlink
      end if @input_files
    end
  end

  # The corresponding SIPp error code is listed after the exception
  class Error < StandardError; end
  class ExitOnInternalCommand < Error; end # 97
  class NoCallsProcessed < Error; end # 99
  class FatalError < Error; end # -1
  class FatalSocketBindingError < Error; end # -2
  class SippGenericError < Error; end # 255 and undocumented errors
end
