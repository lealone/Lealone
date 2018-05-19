class User extends Model {

    constructor(modelTable, modelType) {
       super();
       this.modelTable = modelTable == null ? new ModelTable("TEST", "PUBLIC", "USER") : modelTable;
       this.modelType = (modelType == undefined || modelType == null) ? REGULAR_MODEL : modelType;

       this.name = new PString("NAME", this);
       this.notes = new PString("NOTES", this);
       this.phone = new PInteger("PHONE", this);
       this.id = new PLong("ID", this);
       super.setModelProperties([ this.name, this.notes, this.phone, this.id ]);
       
       this.defineProperty(this.name, "name");
       this.defineProperty(this.notes, "notes");
       this.defineProperty(this.phone, "phone");
       this.defineProperty(this.id, "id");
    }
    
    defineProperty(p, name) { 
        Object.defineProperty(this, name, {
            enumerable: true,
            configurable: true,
            get: function(){
                return p; // 如果返回 p.get()，那么不能再使用流式化风格，只能返回 p，然后在 ModelProperty类中添加 toString()方法
            },
            set: function(newValue){
                p.set(newValue);
            }
        });
    }

}

User.dao = new User(null, ROOT_DAO);

User.parse = function(jsonText) {
    var user = new User();
    JSON.parse(jsonText, function(key, value) {
        switch(key){
        case "NAME":
            return user.name.set(value);
        case "NOTES":
            return user.notes.set(value);
        case "PHONE":
            return user.phone.set(value);
        case "ID":
            return user.id.set(value);
        default:
            return value;
        }
    });
    return user;
}





