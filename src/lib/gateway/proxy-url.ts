const LOOPBACK_HOSTS = new Set(["localhost", "127.0.0.1", "::1"]);

export const resolveStudioProxyGatewayUrl = (upstreamGatewayUrl?: string): string => {
  // Always proxy via Studio same-origin WebSocket bridge when accessed in browser
  const protocol = typeof window !== "undefined" && window.location.protocol === "https:" ? "wss" : "ws";
  const host = typeof window !== "undefined" ? window.location.host : "127.0.0.1:3000";
  return `${protocol}://${host}/api/gateway/ws`;
};

