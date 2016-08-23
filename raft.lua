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

return M
