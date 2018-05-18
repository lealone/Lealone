class User extends Model {

	constructor(modelTable, modelType) {
		super(modelTable == null ? new ModelTable("TEST", "PUBLIC", "USER") : modelTable,
				(modelType == undefined || modelType == null) ? REGULAR_MODEL : modelType);
		this.name = new PString("NAME", this);
        this.notes = new PString("NOTES", this);
        this.phone = new PInteger("PHONE", this);
        this.id = new PLong("ID", this);
        super.setModelProperties([ this.name, this.notes, this.phone, this.id ]);
    }

}

User.dao = new User(null, ROOT_DAO);
