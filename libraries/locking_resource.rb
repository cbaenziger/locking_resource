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
    attribute(:timeout, kind_of: Integer, default:
      lazy { node['locking_resource']['restart_lock_acquire']['timeout'] })
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
      converge_by("serializing #{new_resource.name} via lock") do
        r = run_context.resource_collection.resources(new_resource.resource)

        # to avoid namespace collisions replace spaces in resource name with
        # a colon -- zookeeper's quite permissive on paths:
        # https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkDataModel
        lock_name = new_resource.lock_name or new_resource.name.gsub(' ', ':')
        lock_path = ::File.join(
          node['locking_resource']['restart_lock']['root'],
          lock_name)

        zk_hosts = parse_zk_hosts(node['locking_resource']['zookeeper_servers'])
        unless node['locking_resource']['skip_restart_coordination']
          Chef::Log.info "Acquiring lock #{lock_path}"
          # acquire lock
          got_lock = lock_matches?(zk_hosts, lock_path, new_resource.lock_data)\
            and Chef::Log.info "Found stale lock"
          # intentionally do not use a timeout to avoid leaving a wonky
          # zookeeper object or connection if we interrupt it -- thus we trust
          # the zookeeper object to not wantonly hang
          start_time = Time.now
          while !got_lock && (start_time + new_resource.timeout) >= Time.now
            got_lock = create_node(zk_hosts, lock_path, new_resource.lock_data)\
              and Chef::Log.info 'Acquired new lock'
            sleep(node['locking_resource']['restart_lock_acquire']['sleep_time'])
          end
          # see if we ever got a lock -- if not record it for later
          if !got_lock
            need_rerun(node,lock_path)
          end
        else
          got_lock = false
          Chef::Log.warn 'Restart coordination disabled -- skipping lock ' \
                         "acquisition on #{lock_path}"
        end

        # affect the resource, if we got the lock -- or error
        if got_lock or node['locking_resource']['skip_restart_coordination']
          notifying_block do
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
          raise 'Failed to acquire lock for ' +
                "LockingResource[#{new_resource.name}], path #{lock_path}"
        end
      end
    end

    # Only restart the service if we are holding the lock
    # and the service has not restarted since we got the lock
    def action_serialize_process
      vppo = ::LockingResource::Helper::VALID_PROCESS_PATTERN_OPTS
      raise "Need a process pattern attribute" unless \
        new_resource.process_pattern.length != 0
      raise "Only expect options: #{vppo.keys} but got " \
        "#{new_resource.process_pattern.keys}" if \
        Set.new(new_resource.process_pattern.keys) < \
        Set.new(vppo.keys)
      converge_by("serializing as process #{new_resource.name} via lock") do
        r = run_context.resource_collection.resources(new_resource.resource)
        # convert keys from strings to symbols for process_start_time()
        start_time_arg = new_resource.process_pattern.inject({}) do |memo,(k,v)|
          memo[k.to_sym] = v
          memo
        end
        p_start = process_start_time(start_time_arg)

        l_time = false
        lock_and_rerun = false

        # if the process is not running we do not care about lock management --
        # just run the action
        if p_start
          # questionable if we want to include cookbook_name and recipe_name in
          # the lock as we may have multiple resources with the same name
          lock_path = ::File.join(
            node['locking_resource']['restart_lock']['root'],
            new_resource.name.gsub(' ', ':'))
          zk_hosts = parse_zk_hosts(
            node['locking_resource']['zookeeper_servers'])

          got_lock = lock_matches?(zk_hosts, lock_path, new_resource.lock_data)\
            or return
          l_time = get_node_ctime(zk_hosts, lock_path)
          Chef::Log.warn "Found stale lock" if got_lock
        end

        r_time = rerun_time?(node, lock_path)
        # verify we are holding the lock and need to re-run
        lock_and_rerun = (p_start <= (r_time or Time.new(0)) and l_time) \
          if p_start

        if !p_start or \
           lock_and_rerun or \
           p_start <= (l_time or Time.new(1970))
          Chef::Log.warn "Restarting process: lock time " \
                         "#{l_time}; rerun state #{r_time}; " \
                         "process restarted #{p_start}"
          notifying_block do
            r.run_action new_resource.perform
            r.resolve_notification_references
            new_resource.updated_by_last_action(r.updated)
          end
          clear_rerun(node, lock_path)
        else
          Chef::Log.warn "Not restarting process: lock time #{l_time}; " \
                         "rerun state #{r_time}; " \
                         "process restarted #{p_start}"
        end
        # release_lock will not matter if we are not holding the lock
        release_lock(zk_hosts, lock_path, new_resource.lock_data)
      end
    end
  end
end

