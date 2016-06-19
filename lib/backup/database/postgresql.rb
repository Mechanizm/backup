# encoding: utf-8

module Backup
  module Database
    class PostgreSQL < Base
      class Error < Backup::Error; end

      ##
      # Name of the database that needs to get dumped.
      # To dump all databases, set this to `:all` or leave blank.
      # +username+ must be a PostgreSQL superuser to run `pg_dumpall`.
      attr_accessor :name

      ##
      # Credentials for the specified database
      attr_accessor :username, :password

      ##
      # If set the pg_dump(all) command is executed as the given user
      attr_accessor :sudo_user

      ##
      # Connectivity options
      attr_accessor :host, :port, :socket

      ##
      # Tables to skip while dumping the database.
      # If `name` is set to :all (or not specified), these are ignored.
      attr_accessor :skip_tables

      ##
      # Tables to dump. This in only valid if `name` is specified.
      # If none are given, the entire database will be dumped.
      attr_accessor :only_tables

      ##
      # Check dump command. This in only valid if `name` is specified.
      # Will restore the dump to temporary database `name` + _check_dump and run check_dump_query
      # If the query returns true, the dump is valid, else isn't valid
      attr_accessor :check_dump_query

      ##
      # Additional "pg_dump" or "pg_dumpall" options
      attr_accessor :additional_options

      ##
      # @private Dump file extension
      attr_reader :dump_ext

      def initialize(model, database_id = nil, &block)
        super
        instance_eval(&block) if block_given?

        @name ||= :all
      end

      ##
      # Performs the pgdump command and outputs the dump file
      # in the +dump_path+ using +dump_filename+.
      #
      #   <trigger>/databases/PostgreSQL[-<database_id>].sql[.gz]
      def perform!
        super

        pipeline = Pipeline.new
        @dump_ext = 'sql'

        pipeline << (dump_all? ? pgdumpall : pgdump)

        model.compressor.compress_with do |command, ext|
          pipeline << command
          @dump_ext << ext
        end if model.compressor

        pipeline << "#{ utility(:cat) } > " +
            "'#{ File.join(dump_path, dump_filename) }.#{ dump_ext }'"
        if check_dump_query
          pipeline << create_temporary_database
          pipeline << restore_dump_to_temporary_database
          pipeline << run_check_dump_query
          pipeline << "#{success_drop_temporary_database} || #{failure_drop_temporary_database}"
        end

        pipeline.run
        if pipeline.success?
          log!(:finished)
        else
          raise Error, "Dump Failed!\n" + pipeline.error_messages
        end
      end

      def pgdump
        "#{ password_option }" +
        "#{ sudo_option }" +
        "#{ utility(:pg_dump) } #{ username_option } #{ connectivity_options } " +
        "#{ user_options } #{ tables_to_dump } #{ tables_to_skip } #{ name }"
      end

      def pgdumpall
        "#{ password_option }" +
        "#{ sudo_option }" +
        "#{ utility(:pg_dumpall) } #{ username_option } " +
        "#{ connectivity_options } #{ user_options }"
      end

      def password_option
        "PGPASSWORD=#{ Shellwords.escape(password) } " if password
      end

      def sudo_option
        "#{ utility(:sudo) } -n -u #{ sudo_user } " if sudo_user
      end

      def username_option
        "--username=#{ Shellwords.escape(username) }" if username
      end

      def connectivity_options
        return "--host='#{ socket }'" if socket

        opts = []
        opts << "--host='#{ host }'" if host
        opts << "--port='#{ port }'" if port
        opts.join(' ')
      end

      def user_options
        Array(additional_options).join(' ')
      end
      def temporary_database_option
        "-d #{ Shellwords.escape(temporary_database_name) }"
      end

      def tables_to_dump
        Array(only_tables).map do |table|
          "--table='#{ table }'"
        end.join(' ')
      end

      def tables_to_skip
        Array(skip_tables).map do |table|
          "--exclude-table='#{ table }'"
        end.join(' ')
      end

      def dump_all?
        name == :all
      end

      def create_temporary_database
        psql_execute("CREATE DATABASE #{temporary_database_name};")
      end

      def psql_execute(query)
        "#{ password_option }" +
        "#{ sudo_option }" +
        "#{ utility(:psql) } #{ username_option } #{ connectivity_options } " +
        "-c '#{Shellwords.escape(query)}'"
      end

      def restore_dump_to_temporary_database
        "#{ password_option }" +
        "#{ sudo_option }" +
        "#{ utility(:pg_restore) } #{ username_option } #{ connectivity_options } " +
        "#{temporary_database_option} '#{ File.join(dump_path, dump_filename) }.#{ dump_ext }'"
      end

      def run_check_dump_query
        psql_execute("\\c #{temporary_database_name}; #{check_dump_query}")
      end

      def success_drop_temporary_database
        psql_execute("DROP DATABASE #{temporary_database_name};")
      end

      def failure_drop_temporary_database
        psql_execute("DROP DATABASE #{temporary_database_name}; DUMP IS INVALID")
      end

      def temporary_database_name
        "#{name}_check_dump"
      end
    end
  end
end
