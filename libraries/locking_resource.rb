#
# Cookbook Name:: locking_resource
# Library:: locking_resource
#
# Copyright (C) 2017 Bloomberg Finance L.P.
#
require 'poise'
require 'set'
class Chef
  class Resource::LockingResource < Resource
    include Poise
    provides(:locking_resource)

    actions(:serialize)
    actions(:serialize_process)
    default_action(:serialize)

    attribute(:name, kind_of: String)
    attribute(:lock_name, kind_of: String, default: nil)
    attribute(:resource, kind_of: String, required: true)
    attribute(:perform, kind_of: Symbol, required: true)
    attribute(:timeout, kind_of: Integer, default: \
      lazy { node['locking_resource']['restart_lock_acquire']['timeout'] })
    attribute(:zookeeper_hosts, kind_of: Array, default: \
      lazy { node['locking_resource']['zookeeper_servers'] })
    attribute(:process_pattern, option_collector: true)
    attribute(:lock_data, kind_of: String, default: lazy { node['fqdn'] })

    # XXX should validate node['locking_resource']['zookeeper_servers'] parses
  end

  class Provider::LockingResource < Provider
    include Poise
    require_relative 'helpers.rb'
    include ::LockingResource::Helper
    provides(:locking_resource)

    def action_serialize
      converge_by("serializing #{new_resource.name}") do
        r = run_context.resource_collection.resources(new_resource.resource)

        # to avoid namespace collisions replace spaces in resource name with
        # a colon -- zookeeper's quite permissive on paths:
        # https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkDataModel
        puts "XXX1 #{new_resource.lock_name}"
        lock_name = new_resource.lock_name || new_resource.name.tr_s(' ', ':')
        lock_path = ::File.join(
          node['locking_resource']['restart_lock']['root'],
          lock_name
        )

        zk_hosts = parse_zk_hosts(new_resource.zookeeper_hosts)
        if node['locking_resource']['skip_restart_coordination']
          got_lock = false
          Chef::Log.warn 'Restart coordination disabled -- skipping lock ' \
                         "acquisition on #{lock_path}"
        else
          Chef::Log.info "Acquiring lock #{lock_path}"
          # acquire lock
          # rubocop:disable no-and-or-or
          got_lock = lock_matches?(zk_hosts, lock_path, new_resource.lock_data)\
            and Chef::Log.info 'Found stale lock'
          # rubocop:enable no-and-or-or

          # intentionally do not use a timeout to avoid leaving a wonky
          # zookeeper object or connection if we interrupt it -- thus we trust
          # the zookeeper object to not wantonly hang
          start_time = Time.now
          while !got_lock && (start_time + new_resource.timeout) >= Time.now
            # rubocop:disable no-and-or-or
            got_lock = create_node(zk_hosts, lock_path, new_resource.lock_data)\
              and Chef::Log.info 'Acquired new lock'
            # rubocop:enable no-and-or-or
            sleep(
              node['locking_resource']['restart_lock_acquire']['sleep_time']
            )
            Chef::Log.warn "Sleeping for lock #{lock_path}"
          end
          # see if we ever got a lock -- if not record it for later
          if !got_lock
            Chef::Log.warn "Did not get lock #{lock_path}"
            need_rerun(node, lock_path)
          end
        end

        # affect the resource, if we got the lock -- or error
        if got_lock || node['locking_resource']['skip_restart_coordination']
          notifying_block do
            unless r
              fail "Unable to find resource #{new_resource.resource} in " \
                'resources'
            end
            r.run_action new_resource.perform
            r.resolve_notification_references
            new_resource.updated_by_last_action(r.updated)
            begin
              release_lock(zk_hosts, lock_path, new_resource.lock_data)
            rescue ::LockingResource::Helper::LockingResourceException => e
              Chef::Log.warn e.message
            end
          end
        else
          need_rerun(node, lock_path)
          fail 'Failed to acquire lock for ' \
                "LockingResource[#{new_resource.name}], path #{lock_path}"
        end
      end
    end

    # Only restart the service if we are holding the lock
    # and the service has not restarted since we started trying to get the lock
    def action_serialize_process
      vppo = ::LockingResource::Helper::VALID_PROCESS_PATTERN_OPTS
      raise 'Need a process pattern attribute' unless \
        !new_resource.process_pattern.empty?
      if Set.new(new_resource.process_pattern.keys) < \
        Set.new(vppo.keys)
        raise "Only expect options: #{vppo.keys} but got " \
          "#{new_resource.process_pattern.keys}"
      end
      converge_by("serializing #{new_resource.name} on process") do
        l_time = false
        lock_and_rerun = false

        r = run_context.resource_collection.resources(new_resource.resource)
        zk_hosts = parse_zk_hosts(new_resource.zookeeper_hosts)

        # convert keys from strings to symbols for process_start_time()
        start_time_arg = \
            new_resource.process_pattern.inject({}) do |memo, (k, v)|
          memo[k.to_sym] = v
          memo
        end

        p_start = process_start_time(start_time_arg) || false

        # questionable if we want to include cookbook_name and recipe_name in
        # the lock as we may have multiple resources with the same name
        lock_name = new_resource.lock_name || new_resource.name.tr_s(' ', ':')
        lock_path = ::File.join(
          node['locking_resource']['restart_lock']['root'],
          lock_name
        )

        # if the process is not running we do not care about lock management --
        # just run the action
        if p_start
          # rubocop:disable no-and-or-or
          got_lock = lock_matches?(zk_hosts, lock_path, new_resource.lock_data)\
            or return
          # rubocop:enable no-and-or-or

          l_time = get_node_ctime(zk_hosts, lock_path)
          Chef::Log.warn 'Found stale lock' if got_lock
        end

        r_time = rerun_time?(node, lock_path)
        # verify we are holding the lock and need to re-run
        lock_and_rerun = (p_start <= (r_time || Time.new(0)) && l_time) \
          if p_start

        if !p_start || \
           lock_and_rerun || \
           p_start <= (l_time || Time.new(1970))
          Chef::Log.info 'Restarting process: lock time: ' \
                         "#{l_time}; rerun flag time: #{r_time}; " \
                         "process restarted since lock: #{p_start}"
          notifying_block do
            r.run_action new_resource.perform
            r.resolve_notification_references
            new_resource.updated_by_last_action(r.updated)
            clear_rerun(node, lock_path)
          end
        else
          Chef::Log.warn "Not restarting process: lock time: #{l_time}; " \
                         "rerun flag time: #{r_time}; " \
                         "process restarted since lock: #{p_start}"
        end
        # release_lock will not matter if we are not holding the lock
        begin
          release_lock(zk_hosts, lock_path, new_resource.lock_data)
        rescue ::LockingResource::Helper::LockingResourceException => e
          Chef::Log.warn e.message
        end
      end
    end
  end
end
