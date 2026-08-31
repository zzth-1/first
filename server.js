const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// 静态文件服务（前端页面）
app.use(express.static(path.join(__dirname, 'public')));

// 房间存储：roomId -> Set of WebSocket clients
const rooms = new Map();

// 生成简洁房间号（8位）
function generateRoomId() {
    return uuidv4().slice(0, 8);
}

// 广播房间成员数更新
function broadcastMembers(roomId) {
    const room = rooms.get(roomId);
    if (!room) return;
    const members = room.size;
    const msg = JSON.stringify({
        type: 'members-update',
        roomId,
        members
    });
    for (const client of room) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(msg);
        }
    }
}

wss.on('connection', (ws, req) => {
    const clientIp = req.socket.remoteAddress;
    console.log(`[${new Date().toISOString()}] 新连接: ${clientIp}`);

    let currentRoom = null;

    ws.on('message', (message) => {
        let data;
        try {
            data = JSON.parse(message);
        } catch {
            return;
        }

        const { type, roomId, payload } = data;

        switch (type) {
            case 'create-room': {
                const newRoomId = generateRoomId();
                if (!rooms.has(newRoomId)) {
                    rooms.set(newRoomId, new Set());
                }
                rooms.get(newRoomId).add(ws);
                currentRoom = newRoomId;
                ws.send(JSON.stringify({
                    type: 'room-created',
                    roomId: newRoomId,
                    members: rooms.get(newRoomId).size
                }));
                broadcastMembers(newRoomId);
                console.log(`[${newRoomId}] 房间已创建，创建者 IP: ${clientIp}`);
                break;
            }

            case 'join-room': {
                const targetRoom = roomId;
                if (!targetRoom || !rooms.has(targetRoom)) {
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: '房间不存在，请检查房间号'
                    }));
                    return;
                }
                rooms.get(targetRoom).add(ws);
                currentRoom = targetRoom;
                ws.send(JSON.stringify({
                    type: 'joined',
                    roomId: targetRoom,
                    members: rooms.get(targetRoom).size
                }));
                broadcastMembers(targetRoom);
                console.log(`[${targetRoom}] 新成员加入，IP: ${clientIp}`);
                break;
            }

            case 'file-data': {
                // 接收文件数据（Base64 编码）
                const { fileName, fileSize, fileDataBase64 } = payload || {};
                if (!currentRoom) {
                    ws.send(JSON.stringify({ type: 'error', message: '未加入房间' }));
                    return;
                }
                const room = rooms.get(currentRoom);
                if (!room) return;

                // 转发给房间内所有其他客户端
                const msg = JSON.stringify({
                    type: 'file-receive',
                    fileName,
                    fileSize,
                    fileDataBase64
                });
                for (const client of room) {
                    if (client !== ws && client.readyState === WebSocket.OPEN) {
                        client.send(msg);
                    }
                }
                console.log(`[${currentRoom}] 文件 "${fileName}" (${(fileSize/1024).toFixed(1)}KB) 已转发`);
                break;
            }

            case 'leave': {
                if (currentRoom) {
                    const room = rooms.get(currentRoom);
                    if (room) {
                        room.delete(ws);
                        if (room.size === 0) {
                            rooms.delete(currentRoom);
                            console.log(`[${currentRoom}] 房间已清空，已删除`);
                        } else {
                            broadcastMembers(currentRoom);
                        }
                    }
                }
                break;
            }

            default:
                break;
        }
    });

    ws.on('close', () => {
        console.log(`[${new Date().toISOString()}] 断开连接: ${clientIp}`);
        if (currentRoom) {
            const room = rooms.get(currentRoom);
            if (room) {
                room.delete(ws);
                if (room.size === 0) {
                    rooms.delete(currentRoom);
                    console.log(`[${currentRoom}] 房间已清空，已删除`);
                } else {
                    broadcastMembers(currentRoom);
                }
            }
        }
    });
});

// 获取本机局域网 IP（Windows 适用）
function getLocalIP() {
    const { networkInterfaces } = require('os');
    const nets = networkInterfaces();
    for (const name of Object.keys(nets)) {
        for (const net of nets[name]) {
            // 跳过内部地址和非 IPv4
            if (net.family === 'IPv4' && !net.internal) {
                return net.address;
            }
        }
    }
    return '127.0.0.1';
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    const ip = getLocalIP();
    console.log('\n✅ 服务已启动！');
    console.log(`   本机访问: http://localhost:${PORT}`);
    console.log(`   局域网访问: http://${ip}:${PORT}`);
    console.log(`   (其他设备请使用局域网访问地址)\n`);
    console.log('   按 Ctrl+C 停止服务\n');
});