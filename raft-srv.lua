------- Usage example: -------
-- local raft = require('raft')
-- r = raft({
-- 	login = 'repler',
-- 	password = 'repler',
-- 	debug = true,
-- 	servers = {
--		{ uri = '127.0.0.1:3303' },
--		{ uri = '127.0.0.2:3303' },
--		{ uri = '127.0.0.3:3303' },
--		{ uri = '127.0.0.4:3303' },
-- 	},
-- })

-- r:start()


local obj = require('obj')
local log = require('log')
local fiber = require('fiber')
local msgpack = require('msgpack')
local yaml = require('yaml')

local function bind(func, object)
	if type(func) == 'table' then print(yaml.encode(func)) end
    return function(...) return func(object, ...) end
end


local MS_TO_S = 1/1000

local raft = require('raft')
local M = obj.class({}, 'raft-srv', raft)


function M:_init(cfg)
	self.name = cfg.name or 'default'
	self.debug = cfg.debug or false;
	
	self._election = {
		period_min = cfg.election_period_min or (150 * MS_TO_S),
		period_max = cfg.election_period_max or (300 * MS_TO_S),
		timer_fiber = nil,
		process_fiber = nil,
		ch = fiber.channel(1),
		active = false,
		timeout = 0
	}
	
	self._heartbeat = {
		period = cfg.heartbeat_period or (100 * MS_TO_S),
		active = false,
		fiber = nil,
	}
	
	self._debugging = {
		period = cfg.debug_log_period or 5,
		active = false,
		fiber = nil,
	}
	
	self._state_wait = {
		timeout = cfg.state_wait_timeout or 5,
		fibers = {},
		fibers_active = {},
	}
	
	self.MODES = {
		EMBEDDED = 'embedded',
		STANDALONE = 'standalone',
	}
	
	if cfg.conn_pool then
		self.mode = self.MODES.EMBEDDED
		self._pool = cfg.conn_pool
		assert(cfg.nodes ~= nil, 'No nodes specified while connection pool provided')
		self._pool_nodes = cfg.nodes
		self._nodes_count = #cfg.nodes
	else
		self.mode = self.MODES.STANDALONE
		box.error({reason = 'Not supported anymore'})
		
		-- local cp = require('shardconnpool')
		-- self._pool = cp {
		-- 	name = 'raft-'.. self.name..'-pool',
		-- 	login = cfg.login,
		-- 	password = cfg.password,
		-- 	servers = cfg.servers,
		-- }
		-- self._nodes_count = #cfg.servers
		-- self._pool.on_connected = bind(self._pool_on_connected, self)
		-- self._pool.on_connected_one = bind(self._pool_on_connected_one, self)
		-- self._pool.on_disconnect_one = bind(self._pool_on_disconnect_one, self)
		-- self._pool.on_disconnect = bind(self._pool_on_disconnect, self)
	end
	
	self.FUNC = self:_make_global_funcs({
		'request_vote',
		'heartbeat',
		'is_leader',
		'get_leader',
		'info',
		'state_wait',
	})
	
	self.S = {
		IDLE = 'idle',
		FOLLOWER = 'follower',
		CANDIDATE = 'candidate',
		LEADER = 'leader'
	}
	
	self.pool = self:_pool_init_functions()
	self.active_nodes_count = 0
	self.id = box.info.server.id
	self.uuid = box.info.server.uuid
	self.prev_state = nil
	self.state = self.S.IDLE
	self.term = 0
	self.prev_leader = msgpack.NULL
	self.leader = msgpack.NULL
	self.nodes_info = {}
	for _, state in pairs(self.S) do
		self.nodes_info[state] = {}
	end
	
	self._vote_count = 0
	self._preferred_leader_uuid = msgpack.NULL
	self._internal_state_channels = setmetatable({}, {__mode='kv'})
end

function M:_make_global_funcs(func_names)
	local F = {}
	if _G.raft == nil then
		_G.raft = {}
	end
	if _G.raft[self.name] ~= nil then
		log.warn("[raft-srv] Another raft." .. self.name .. " callbacks detected in _G. Replacing them.")
	end
	_G.raft[self.name] = {}
	for _,f in ipairs(func_names) do
		if self[f] then
			_G.raft[self.name][f] = bind(self[f], self)
			F[f] = 'raft.' .. self.name .. '.' .. f
		else
			log.warn("[raft-srv] No function '" .. f .. "' found. Skipping...")
		end
	end
	return F
end

function M:_set_state(new_state)
	if new_state ~= self.state then
		self.prev_state = self.state
		self.state = new_state
		log.info("[raft-srv] State: %s -> %s", self.prev_state, self.state)
		for _,v in pairs(self._internal_state_channels) do
			v:put('state_change')
		end
	end
end

function M:_set_leader(new_leader)
	if new_leader == nil or self.leader == nil or new_leader.uuid ~= self.leader.uuid then
		self.prev_leader = self.leader
		self.leader = new_leader
		
		if self.prev_leader ~= nil and self.leader ~= nil then
			for _,v in pairs(self._internal_state_channels) do
				v:put('leader_change')
			end
		end
	end
	
end

function M:start()
	log.info("[raft-srv] Starting raft...")
	if self.mode == self.MODES.STANDALONE then
		log.info("[raft-srv] Connecting to pool...")
		self._pool:connect()
	end
	
	self:start_election_timer()
	
	if self.debug then
		self:start_debugger()
	end
end

function M:stop()
	self:stop_election_timer()
	self:stop_heartbeater()
	self:stop_debugger()
	if self.mode == self.MODES.STANDALONE then
		-- self._pool:disconnect()
	end
	
	_G.raft[self.name] = nil
end

function M:_new_election_timeout()
	self._election.timeout = math.random(self._election.period_min, self._election.period_max)
	return self._election.timeout
end

function M:start_election_timer()
	if self._election.timer_fiber == nil then
		log.info('[raft-srv] Started election timer')
		self._election.timer_fiber = fiber.create(function(self)
			fiber.self():name('election_fiber')
			
			self._election.active = true
			while self._election.active do
				fiber.testcancel()
				local timeout = self:_new_election_timeout()
				-- log.info('Waiting elections for %fs', timeout)
				local v = self._election.ch:get(timeout)
				if v == nil then
					if self.debug then log.info("[raft-srv] Timeout exceeded. Starting elections.") end
					
					self._election.process_fiber = fiber.create(self._initiate_elections, self)
					self._election.process_fiber:name('election_process_fiber')
					
					self._election.active = false
				end
			end
		end, self)
	end
end

function M:stop_election_timer()
	if self._election.timer_fiber ~= nil then
		if self._election.timer_fiber:status() ~= 'dead' then
			self._election.timer_fiber:cancel()
		end
		self._election.timer_fiber = nil
	end
	self._election.active = false
	log.info('[raft-srv] Stopped election timer')
end

function M:restart_election_timer()
	self:stop_election_timer()
	self:start_election_timer()
end

function M:reset_election_timer()
	if self._election.active then
		self._election.ch:put(1)
	end
end

function M:stop_election_process()
	if self._election.process_fiber ~= nil and self._election.process_fiber:status() ~= 'dead' then
		self._election.process_fiber:cancel()
		log.info('[raft-srv] Stopped election process')
	end
	self._election.process_fiber = nil
end

function M:_is_good_for_candidate()
	-- TODO: check all nodes and see if current node is good to be a leader, then return true
	-- TODO: if not, return false
	
	local r = self.pool.eval("return box.info")
	if not r then return true end
	
	-- for now it is that lag is the least
	local minimum = {
		uuid = self.uuid,
		lag = nil
	}
	for node_uuid,response in pairs(r) do
		local success, resp = unpack(response)
		if success then
			-- print(yaml.encode(resp))
			if resp.replication.status ~= 'off' and resp.replication.lag ~= nil then
				if self.debug then log.info("[raft-srv][lag] id = %d; uuid = %s; lag = %f", resp.server.id, resp.server.uuid, resp.replication.lag) end
				if self.debug then
					log.info("[raft-srv][lag] condition1: %d", minimum.lag == nil and 1 or 0)
					if minimum.lag ~= nil then
						log.info("[raft-srv][lag] condition2: %d", (resp.replication.lag <= minimum.lag and resp.server.uuid == self.uuid) and 1 or 0)
						log.info("[raft-srv][lag] condition3: %d", resp.replication.lag < minimum.lag and 1 or 0)
					end
				end
				if minimum.lag == nil or (resp.replication.lag <= minimum.lag and resp.server.uuid == self.uuid) or resp.replication.lag < minimum.lag then
					minimum.uuid = resp.server.uuid
					minimum.lag = resp.replication.lag
				end
			end
		else
			log.warn('[raft-srv] Error whlie determining a good candidate for node %s: %s', node_uuid, resp)
		end
	end
	if self.debug then
		if minimum.lag ~= nil then
			log.info("[raft-srv][lag] minimum = {uuid=%s; lag=%d}", minimum.uuid, minimum.lag)
		else
			log.info("[raft-srv][lag] lag couldn't been determined. uuid = ", minimum.uuid)
		end
	end
	
	-- self._preferred_leader_uuid = minimum.uuid
	return minimum.uuid == self.uuid
end

function M:_initiate_elections()
	fiber.yield()
	self:stop_election_timer()
	
	self:_set_leader(msgpack.NULL)
	
	if not self:_is_good_for_candidate() then
		if self.debug then log.info("[raft-srv] node %s is not good to be a candidate", self.uuid) end
		self:start_election_timer()
		return
	else
		if self.debug then
			log.ifo("[raft-srv] node %s is good to be a candidate. Active nodes = %d. Nodes count = %d", self.uuid, self.active_nodes_count, self._nodes_count)
		end
	end
	
	if self._nodes_count ~= 1 and self.active_nodes_count == 1 then
		log.info("[raft-srv] node %s is left by itself", self.uuid)
		self:_set_state(self.S.IDLE)
		self:start_election_timer()
		return
	end
	
	self.term = self.term + 1
	self:_set_state(self.S.CANDIDATE)
	
	local r = self.pool.call(self.FUNC.request_vote, self.term, self.uuid)
	if not r then return end
	
	if self.debug then print(yaml.encode(r)) end
	-- finding majority
	for _,response in pairs(r) do
		local success, resp = unpack(response)
		if success and resp then
			local decision = resp[1][1]
			
			local vote = decision == "ack" and 1 or 0
			self._vote_count = self._vote_count + vote
		end
	end
	
	if self.debug then log.info("[raft-srv] resulting votes count: %d/%d", self._vote_count, self._nodes_count) end
	
	if self._vote_count > self._nodes_count / 2 then
		-- elections won
		if self.debug then log.info("[raft-srv] node %d won elections [uuid = %s]", self.id, self.uuid) end
		self:_set_state(self.S.LEADER)
		self:_set_leader({ id=self.id, uuid=self.uuid })
		self._vote_count = 0
		self:stop_election_timer()
		self:start_heartbeater()
	else
		-- elections lost
		if self.debug then log.info("[raft-srv] node %d lost elections [uuid = %s]", self.id, self.uuid) end
		self.term = self.term - 1
		self:_set_state(self.S.IDLE)
		self:_set_leader(msgpack.NULL)
		self._vote_count = 0
		self:start_election_timer()
	end
	-- self._preferred_leader_uuid = msgpack.NULL;
end

function M:start_heartbeater()
	if self._heartbeat.fiber == nil then
		self._heartbeat.fiber = fiber.create(function(self)
			fiber.self():name("heartbeat_fiber")
			
			self._heartbeat.active = true
			while self._heartbeat.active do
				if self.debug then log.info("[raft-srv] performing heartbeat") end
				fiber.create(function(self)  -- don't wait for heartbeat to finish!
					local r = self.pool.call(self.FUNC.heartbeat, self.term, self.uuid, self.leader)
				end, self)
				fiber.sleep(self._heartbeat.period)
			end
		end, self)
	end
end

function M:stop_heartbeater()
	-- print("---- stopping heartbeater 1")
	if self._heartbeat.fiber then
		-- print("---- stopping heartbeater 2")
		self._heartbeat.active = false
		self._heartbeat.fiber:cancel()
		self._heartbeat.fiber = nil
	end
end

function M:restart_heartbeater()
	self:stop_heartbeater()
	self:start_heartbeater()
end

function M:start_debugger()
	local logger = function()
		local s = "[raft-srv] state=%s; term=%d; id=%d; uuid=%s; leader=%s"
		local _nil = "nil"
		local leader_str = _nil
		if self.leader ~= nil then
			leader_str = self.leader.uuid
		end
		return string.format(s, self.state or _nil,
								self.term or _nil,
								self.id or _nil,
								self.uuid or _nil,
								leader_str)
	end
	if self._debugging.fiber == nil then
		self._debugging.active = true
		self._debugging.fiber = fiber.create(function()
			fiber.self():name("debug_fiber")
			while self._debugging.active do
				log.info(logger())
				fiber.sleep(self._debugging.period)
			end
		end)
	end
end

function M:stop_debugger()
	if self._debugging.fiber ~= nil then
		self._debugging.active = false
		self._debugging.fiber:cancel()
		self._debugging.fiber = nil
	end
end

function M:call_on_leader(func_name, ...)
	if self.leader == nil or self.leader.uuid == nil then
		log.error('[raft-srv] Cannot call on leader, when leader == nil')
	end
	local node = self._pool:get_by_uuid(self.leader.uuid)
	
	if node ~= nil then
		return node.conn:call(func_name, ...)
	else
		log.error('[raft-srv] leader_node is nil!')
		return nil
	end
end

function M:get_info(uuid)
	while true do
		local node = self._pool:get_by_uuid(uuid)
		if node == nil then
			log.warn("[raft-srv][get_info] Lost node with uuid: %s", uuid)
			break
		end
		local r, e = pcall(node.conn.call, node.conn, self.FUNC.info)
		if r and e then
			local response = e[1][1]
			return response
		else
			log.warn("[raft-srv] Error while info on node %s. %s:%s", uuid, r, e)
			fiber.sleep(0.1)
		end
	end
end

function M:_update_nodes_stat(node, state_info)
	local uuid = node.uuid
	
	for state, nodes in pairs(self.nodes_info) do
		for n_uuid, n in pairs(nodes) do
			if n_uuid == uuid then
				nodes[uuid] = nil
			end
		end
	end
		
	if state_info ~= nil then
		local state = state_info.state
		self.nodes_info[state] = self.nodes_info[state] or {}
		self.nodes_info[state][uuid] = true
	end
end

function M:start_state_wait(uuid)
	if uuid == nil then
		log.error('[raft-srv] Tried to start state_wait on nil uuid')
		return
	end
	
	self._state_wait.fibers_active[uuid] = true
	
	self._state_wait.fibers[uuid] = fiber.create(function()
		fiber.self():name('fiber:state_wait')
		log.info('[raft-srv] Started state_wait on node %s', uuid)
		
		while self._state_wait.fibers_active[uuid] do
			fiber.testcancel()
			if self.debug then log.info('[raft-srv] Sending state_wait for node %s', uuid) end
			local node = self._pool:get_by_uuid(uuid)
			if node == nil then
				log.warn("[raft-srv][state_wait] Lost node with uuid: %s", uuid)
				break
			end
			local r, e = pcall(node.conn.call, node.conn, self.FUNC.state_wait, self._state_wait.timeout)
			
			if r and e then
				local response = e[1][1]
				-- print(uuid, require('yaml').encode(response), require('yaml').encode(self.nodes_info))
				local new_info = response.info

				self:_update_nodes_stat(node, new_info)
			else
				log.warn("[raft-srv] Error while state_wait on node %s. %s:%s", uuid, r, e)
			end
		end
		
	end)
end


function M:stop_state_wait(uuid)
	if uuid == nil then return end
	
	log.info('[raft-srv] Stopped state_wait on node %s', uuid)
	-- if self._state_wait.fibers[uuid] ~= nil then
	-- 	self._state_wait.fibers[uuid]:cancel()
	-- end
	self._state_wait.fibers[uuid] = nil
	self._state_wait.fibers_active[uuid] = false
end

---------------- Global functions ----------------

function M:request_vote(term, uuid)
	self:reset_election_timer()
	
	local res
	if self._preferred_leader_uuid ~= msgpack.NULL then
		if self._preferred_leader_uuid == uuid then
			res = "ack"
		else
			res = "nack"
		end
	else
		if self.uuid == uuid or self.term < term then  -- newer one
			res = "ack"
		else
			res = "nack"
		end
	end
	
	if self.debug then log.info("[raft-srv] --> request_vote: term = %d; uuid = %s; res = %s", term, uuid, res) end
	return res
end

function M:heartbeat(term, uuid, leader)
	if self.uuid ~= uuid and self.term <= term then
		self:start_election_timer()
		self:stop_election_process()
		self:reset_election_timer()
		self:stop_heartbeater()
		self:_set_state(self.S.FOLLOWER)
		self._vote_count = 0
		self.term = term
		self:_set_leader(leader)
		self._preferred_leader_uuid = msgpack.NULL
		if self.debug then log.info("[raft-srv] --> heartbeat: term = %d; uuid = %s; leader_id = %d;", term, uuid, leader.id) end
	end
	return "ack"
end

function M:is_leader(uuid)
	if uuid == nil then
		uuid = self.uuid
	end
	return self.leader ~= nil and self.leader.uuid == uuid
end

function M:get_leader()
	return self.leader
end

function M:get_leaders_uuids()
	local leader = self.leader and self.leader.uuid
	if leader ~= nil then
		return { leader }
	end
	return {}
end

function M:get_nodes_by_state(state, nodes)
	-- Get all nodes by state
	local present_nodes = self.nodes_info[state]
	if nodes == nil then
		nodes = {}
	end
	for uuid, active in pairs(present_nodes) do
		if active then
			local node = self._pool:get_by_uuid(uuid)
			if node ~= nil then
				table.insert(nodes, node)
			end
		end
	end
	return nodes
end

function M:get_leader_nodes()
	-- Get all nodes in state leader
	return self:get_nodes_by_state(self.S.LEADER)
end

function M:get_follower_nodes()
	-- Get all nodes in state follower
	return self:get_nodes_by_state(self.S.FOLLOWER)
end

function M:get_aleader_nodes()
	-- Get all nodes, but leaders first
	local nodes = self:get_leader_nodes()
	for _, state in pairs(self.S) do
		if state ~= self.S.LEADER then
			nodes = self:get_nodes_by_state(state, nodes)
		end
	end
	return nodes
end

function M:get_afollower_nodes()
	-- Get all nodes, but leaders first
	local nodes = self:get_follower_nodes()
	
	for _, state in pairs(self.S) do
		if state ~= self.S.FOLLOWER then
			nodes = self:get_nodes_by_state(state, nodes)
		end
	end
	return nodes
end



function M:info(pack_to_tuple)
	if pack_to_tuple == nil then
		pack_to_tuple = true
	end
	local info = {
		type = self.___name,
		id = self.id,
		uuid = self.uuid,
		prev_state = self.prev_state,
		state = self.state,
		leader = self.leader,
	}
	if pack_to_tuple == true then
		return box.tuple.new{info}
	else
		return info
	end
end

function M:state_wait(timeout)
	timeout = tonumber(timeout) or 0
	
	local ch = fiber.channel(5)
	self._internal_state_channels[ch] = ch
	local m = ch:get(timeout)
	self._internal_state_channels[ch] = nil
	
	local event = 'none'
	if m ~= nil then
		event = m
	end
	
	if self.debug then log.info("[raft-srv] --> state_wait()") end
	local data = {
		event = event,
		state = self.state,
		prev_state = self.prev_state,
		info = self:info(false),
	}
	return box.tuple.new{data}
end

---------------- pool functions ----------------

function M:_pool_init_functions()
	local pool = {}
	if self.mode == self.MODES.EMBEDDED then
		pool.eval = bind(self.pool_eval_embedded, self)
		pool.call = bind(self.pool_call_embedded, self)
	else
		pool.eval = bind(self.pool_eval_standalone, self)
		pool.call = bind(self.pool_call_standalone, self)
	end
	return pool
end

function M:pool_eval_embedded(...)
	return self._pool:eval_nodes(self._pool_nodes, ...)
end

function M:pool_eval_standalone(...)
	return self._pool:eval(...)
end

function M:pool_call_embedded(...)
	return self._pool:call_nodes(self._pool_nodes, ...)
end

function M:pool_call_standalone(...)
	return self._pool:call(...)
end

function M:_pool_on_connected_one(node)
	log.info('[raft-srv] on_connected_one [%s/%s]',node.peer,node.uuid)
	self.active_nodes_count = self.active_nodes_count + 1
	local srv_raft_info = self:get_info(node.uuid)
	if srv_raft_info ~= nil then  -- then node is disconnected
		self:_update_nodes_stat(node, srv_raft_info)
		
		self:start_state_wait(node.uuid)
	end
end

function M:_pool_on_disconnect_one(node)
	log.info('[raft-srv] on_disconnect_one [%s/%s]!',node.peer,node.uuid)
	self.active_nodes_count = self.active_nodes_count - 1
	self:_update_nodes_stat(node, nil)
	-- self:stop_state_wait(node.uuid)
end

function M:_pool_on_connected()
	log.info('[raft-srv] on_connected all!')
end

function M:_pool_on_disconnect()
	log.info('[raft-srv] on_disconnect all!')
	
end


return M

