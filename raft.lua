local obj = require('obj')

local M = obj.class({}, 'raft')

function M:_init(cfg)
	
end

function M:start()
	box.error{reason="Not implemented"}
end

function M:stop()
	box.error{reason="Not implemented"}
end

function M:get_leaders_uuids()
	box.error{reason="Not implemented"}
end

function M:get_leader_nodes()
	box.error{reason="Not implemented"}
end

function M:get_nodes_by_state(state, nodes)
	-- Get all nodes by state
end

function M:get_follower_nodes()
	-- Get all nodes in state follower
end

function M:get_aleader_nodes()
	-- Get all nodes, but leaders first
end

function M:get_afollower_nodes()
	-- Get all nodes, but leaders first
end

return M
