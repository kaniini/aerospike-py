AS_ERR_TYPE_NOT_SUPPORTED = -7
AS_ERR_COMMAND_REJECTED = -6
AS_ERR_QUERY_TERMINATED = -5
AS_ERR_SCAN_TERMINATED = -4
AS_ERR_INVALID_NODE_ERROR = -3
AS_ERR_PARSE_ERROR = -2
AS_ERR_SERIALIZE_ERROR = -1

AS_ERR_OK = 0

AS_ERR_SERVER_ERROR = 1
AS_ERR_KEY_NOT_FOUND_ERROR = 2
AS_ERR_GENERATION_ERROR = 3
AS_ERR_PARAMETER_ERROR = 4
AS_ERR_KEY_EXISTS_ERROR = 5
AS_ERR_BIN_EXISTS_ERROR = 6
AS_ERR_CLUSTER_KEY_MISMATCH = 7
AS_ERR_SERVER_MEM_ERROR = 8

AS_ERR_INVALID_NAMESPACE = 20


error_table = {
    AS_ERR_TYPE_NOT_SUPPORTED: "Type not supported",
    AS_ERR_COMMAND_REJECTED: "Command rejected",
    AS_ERR_QUERY_TERMINATED: "Query terminated",
    AS_ERR_SCAN_TERMINATED: "Scan terminated",
    AS_ERR_INVALID_NODE_ERROR: "Invalid node",
    AS_ERR_PARSE_ERROR: "Parse error",
    AS_ERR_SERIALIZE_ERROR: "Serialize error",

    AS_ERR_OK: "OK",

    AS_ERR_SERVER_ERROR: "Unspecified server error",
    AS_ERR_KEY_NOT_FOUND_ERROR: "Specified key could not be located",
    AS_ERR_GENERATION_ERROR: "Invalid generation specified",
    AS_ERR_PARAMETER_ERROR: "Invalid parameter specified",
    AS_ERR_KEY_EXISTS_ERROR: "Specified key already exists",
    AS_ERR_BIN_EXISTS_ERROR: "Specified bin already exists",
    AS_ERR_CLUSTER_KEY_MISMATCH: "Cluster key does not match",
    AS_ERR_SERVER_MEM_ERROR: "Out of memory",

    AS_ERR_INVALID_NAMESPACE: "Invalid namespace",
}


class ASMSGProtocolException(Exception):
    def __init__(self, result_code):
        super(ASMSGProtocolException, self).__init__(error_table.get(result_code, '??? [%d]' % result_code))
