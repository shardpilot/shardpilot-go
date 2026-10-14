package shardpilot

func validEventSchemaVersion(version *int) bool {
	return version == nil || (*version >= 1 && *version <= 65535)
}

func (c *Client) validateHostSchemaVersion(version *int) error {
	if validEventSchemaVersion(version) {
		return nil
	}
	c.stats.dropped.Add(1)
	c.stats.setLastError("schema_version_invalid")
	return ErrSchemaVersionInvalid
}
