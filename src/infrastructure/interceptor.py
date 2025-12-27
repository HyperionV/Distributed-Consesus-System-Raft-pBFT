"""
Partition Interceptor - Network Simulation Middleware
Intercepts gRPC calls and blocks traffic from specified IPs
"""
import grpc


class PartitionInterceptor(grpc.ServerInterceptor):
    """
    gRPC server interceptor that simulates network partitions.
    Blocks incoming RPCs from IPs in the node's blocked_ips set.
    """
    
    def __init__(self, node):
        self.node = node
    
    def intercept_service(self, continuation, handler_call_details):
        """
        Intercept incoming RPC calls and block if sender is partitioned.
        """
        # Get peer address from metadata
        peer = handler_call_details.invocation_metadata
        peer_ip = self._extract_peer_ip(peer)
        
        # Check if this peer is blocked
        if peer_ip and peer_ip in self.node.blocked_ips:
            self.node.logger.debug(f"Blocked RPC from partitioned peer: {peer_ip}")
            return self._create_abort_handler()
        
        # Allow the call through
        return continuation(handler_call_details)
    
    def _extract_peer_ip(self, metadata):
        """Extract peer IP from gRPC metadata"""
        if not metadata:
            return None
        
        # Look for peer address in metadata
        for key, value in metadata:
            if key == 'peer' or key == ':authority':
                # Extract IP from "ipv4:127.0.0.1:port" or "127.0.0.1:port"
                if ':' in value:
                    parts = value.split(':')
                    if parts[0] == 'ipv4':
                        return parts[1]
                    return parts[0]
        return None
    
    def _create_abort_handler(self):
        """Create a handler that aborts the RPC"""
        def abort_handler(request, context):
            context.abort(grpc.StatusCode.UNAVAILABLE, "Network partition simulated")
        return grpc.unary_unary_rpc_method_handler(abort_handler)


class ClientPartitionInterceptor(grpc.UnaryUnaryClientInterceptor):
    """
    gRPC client interceptor that simulates network partitions on outgoing calls.
    Blocks outgoing RPCs to IPs in the node's blocked_ips set.
    """
    
    def __init__(self, node):
        self.node = node
    
    def intercept_unary_unary(self, continuation, client_call_details, request):
        """
        Intercept outgoing RPC calls and block if target is partitioned.
        """
        # Extract target from metadata/target
        target = client_call_details.method
        target_ip = self._extract_target_ip(client_call_details)
        
        if target_ip and target_ip in self.node.blocked_ips:
            self.node.logger.debug(f"Blocked outgoing RPC to partitioned peer: {target_ip}")
            # Raise an exception to simulate network failure
            raise grpc.RpcError()
        
        return continuation(client_call_details, request)
    
    def _extract_target_ip(self, client_call_details):
        """Extract target IP from call details"""
        # This would need to be passed through metadata in a real implementation
        return None
