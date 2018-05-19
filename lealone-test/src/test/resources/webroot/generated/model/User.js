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
    }

}

User.dao = new User(null, ROOT_DAO);

User.getFormInstance = function(){
    var user = new User();
    var userProxy = {
    }
    userProxy.insert = function(cb) { user.insert(cb) }
    userProxy.delete = function(cb) { user.delete(cb) }
    userProxy.update = function(cb) { user.update(cb) }
    userProxy.findOne = function(cb) { user.findOne(cb) }
    
    Object.defineProperty(userProxy, "name", {
        get: function(){
            return user.name.get();
        },
        set: function(newValue){
            user.name.set(newValue);
        }
    });
    Object.defineProperty(userProxy, "notes", {
        get: function(){
            return user.notes.get();
        },
        set: function(newValue){
            user.notes.set(newValue);
        }
    });
    Object.defineProperty(userProxy, "phone", {
        get: function(){
            return user.phone.get();
        },
        set: function(newValue){
            user.phone.set(newValue);
        }
    });
    
    return userProxy;
}
