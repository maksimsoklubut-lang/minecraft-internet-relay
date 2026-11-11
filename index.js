// Minecraft Java 1.12.2 HTML — internet relay for two players per room.
// Deploy this Worker on your OWN Cloudflare account.
// Messages are forwarded as-is; the server does not create game worlds.
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (request.method === 'GET' && url.pathname === '/') {
      return new Response('Minecraft112 relay online. Open the game to connect.', {
        headers: { 'content-type': 'text/plain; charset=utf-8', 'access-control-allow-origin': '*' }
      });
    }
    if (request.headers.get('Upgrade')?.toLowerCase() !== 'websocket') {
      return new Response('WebSocket connection required', { status: 426 });
    }
    const matched = /^\/room\/(mc112-[0-9a-f]{48})$/.exec(url.pathname);
    const role = url.searchParams.get('role');
    if (!matched || !['host', 'client'].includes(role)) {
      return new Response('Invalid room or role', { status: 400 });
    }
    const id = env.ROOMS.idFromName(matched[1]);
    return env.ROOMS.get(id).fetch(request);
  }
};

// Durable Objects provide one stateful relay instance for each room.
// ctx.acceptWebSocket enables hibernation when there is no traffic.
export class MinecraftRoom {
  constructor(ctx, env) {
    this.ctx = ctx;
  }
  peer(role) {
    return this.ctx.getWebSockets().find(socket =>
      socket.readyState === WebSocket.OPEN &&
      socket.deserializeAttachment()?.role === role
    );
  }
  async fetch(request) {
    const role = new URL(request.url).searchParams.get('role');
    const existing = this.peer(role);
    const host = this.peer('host');
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({role, rejected: Boolean(existing || (role === 'client' && !host))});
    if (existing) {
      server.send(JSON.stringify({__relay:'error',code:role==='host'?'host-taken':'room-full'}));
      server.close(1008,'Room full');
    } else if (role === 'client' && !host) {
      server.send(JSON.stringify({__relay:'error',code:'host-missing'}));
      server.close(1008,'Host not connected');
    } else if (role === 'host') {
      server.send(JSON.stringify({__relay:'waiting'}));
    } else {
      server.send(JSON.stringify({__relay:'paired'}));
      host.send(JSON.stringify({__relay:'paired'}));
    }
    return new Response(null, {status:101,webSocket:client});
  }
  webSocketMessage(ws, data) {
    // The browser game uses < 10KiB chunks; do not allow unbounded frames.
    if (typeof data !== 'string' || data.length > 128*1024) {
      ws.close(1009,'Packet too large');
      return;
    }
    const role = ws.deserializeAttachment()?.role;
    if (!['host','client'].includes(role)) {ws.close(1008,'Invalid client');return;}
    const other = this.peer(role === 'host' ? 'client':'host');
    // The caller must be one of the active sockets, not a rejected third party.
    if (this.peer(role) !== ws || !other) return;
    if (other.bufferedAmount > 2*1024*1024) {
      ws.close(1013,'Receiver is too slow');
      return;
    }
    other.send(data);
  }
  webSocketClose(ws) {
    const attachment = ws.deserializeAttachment();
    if (attachment?.rejected) return;
    const role = attachment?.role;
    // A closed socket is no longer returned by peer().
    const other = this.peer(role === 'host'?'client':'host');
    if (other) {
      other.send(JSON.stringify({__relay:'peer-left'}));
      other.close(1000,'Peer disconnected');
    }
  }
  webSocketError(ws) {
    try {ws.close(1011,'Relay socket error')} catch (_) {}
  }
}
