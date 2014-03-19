var MessageManager = require("./libs/message/message_manager").MessageManager;

var manager = new MessageManager();
manager.listen(2900);
