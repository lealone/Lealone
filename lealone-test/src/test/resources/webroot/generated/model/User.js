class User extends Model {

    constructor(modelTable, modelType) {
        super();
        this.modelTable = modelTable == null ? new ModelTable("TEST", "PUBLIC", "USER") : modelTable;
        this.modelType = (modelType == undefined || modelType == null) ? REGULAR_MODEL : modelType;

        this.name = new PString("NAME", this);
        this.notes = new PString("NOTES", this);
        this.phone = new PInteger("PHONE", this);
        this.id = new PLong("ID", this);
        
        Object.defineProperty(this, "p_name", {
            get: function(){
                return this.name.get();
            },
            set: function(newValue){
                this.name.set(newValue);
            }
        });
        Object.defineProperty(this, "p_notes", {
            get: function(){
                return this.notes.get();
            },
            set: function(newValue){
                this.notes.set(newValue);
            }
        });
        Object.defineProperty(this, "p_phone", {
            get: function(){
                return this.phone.get();
            },
            set: function(newValue){
                this.phone.set(newValue);
            }
        });
        super.setModelProperties([ this.name, this.notes, this.phone, this.id ]);
    }

}

User.dao = new User(null, ROOT_DAO);
