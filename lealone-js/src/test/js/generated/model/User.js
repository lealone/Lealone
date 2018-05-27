class User extends Model {
    constructor(modelTable, modelType) {
       super(modelTable || new ModelTable("TEST", "PUBLIC", "USER"), modelType); 

       this.name = new PString("NAME", this);
       this.notes = new PString("NOTES", this);
       this.phone = new PInteger("PHONE", this);
       this.id = new PLong("ID", this);
    }
}

User.dao = new User(null, ROOT_DAO);

User.parse = function(jsonText) { return new User().parse(jsonText); }
