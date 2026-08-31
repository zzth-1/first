const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const os = require('os');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// 静态文件服务
app.use(express.static(path.join(__dirname, 'public')));

// 房间存储
const rooms = new Map();

// 生成房间号
function generateRoomId() {
    return uuidv4().slice(0, 8);
}

// 广播成员数
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

// 获取所有可用IP地址（兼容 Linux 和 Windows）
function getAllIPs() {
    const ips = [];
    const nets = os.networkInterfaces();
    
    for (const name of Object.keys(nets)) {
        for (const net of nets[name]) {
            // 只获取 IPv4 且不是内部地址
            if (net.family === 'IPv4' && !net.internal) {
                // 过滤掉 Docker、虚拟机等虚拟网卡
                if (name.includes('docker') || 
                    name.includes('veth') || 
                    name.includes('br-') || 
                    name.includes('VirtualBox') ||
                    name.includes('VMware')) {
                    continue;
                }
                ips.push({
                    name: name,
                    address: net.address
                });
            }
        }
    }
    
    // 如果没有找到非内部IP，返回 localhost
    if (ips.length === 0) {
        ips.push({
            name: 'localhost',
            address: '127.0.0.1'
        });
    }
    
    return ips;
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
                const { fileName, fileSize, fileDataBase64 } = payload || {};
                if (!currentRoom) {
                    ws.send(JSON.stringify({ type: 'error', message: '未加入房间' }));
                    return;
                }
                const room = rooms.get(currentRoom);
                if (!room) return;

                // 限制文件大小（50MB）
                const maxSize = 50 * 1024 * 1024;
                if (fileSize > maxSize) {
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: `文件过大（${(fileSize/1024/1024).toFixed(1)}MB），限制 50MB`
                    }));
                    return;
                }

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

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    const ips = getAllIPs();
    console.log('\n✅ 服务已启动！');
    console.log(`   本机访问: http://localhost:${PORT}`);
    console.log('\n   📱 手机端访问（选择其中一个IP）:');
    
    ips.forEach((ip, index) => {
        console.log(`   ${index + 1}. http://${ip.address}:${PORT}  (${ip.name})`);
    });
    
    console.log('\n   💡 提示:');
    console.log('   - 手机和电脑需要连接同一个 WiFi');
    console.log('   - 如果无法访问，检查防火墙是否允许端口 3000');
    console.log('   - 在手机浏览器中直接输入上面的地址');
    console.log('\n   按 Ctrl+C 停止服务\n');
});

// API: 获取所有IP地址
app.get('/api/ips', (req, res) => {
    const ips = getAllIPs();
    res.json(ips);
});
