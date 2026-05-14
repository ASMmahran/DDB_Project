const express = require('express');
const app = express();
app.use(express.json());

app.post('/process', (req, res) => {
    const text = req.body.data || "";
    // عملية خاصة: تشفير البيانات
    const encrypted = Buffer.from(text + "_SALT_FCI").toString('base64');
    res.json({
        special_result: encrypted,
        stack: "Node.js/Express"
    });
});

app.listen(5002, () => console.log('Node.js Worker on port 5002'));