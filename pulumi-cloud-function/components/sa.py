from .shared_imports import (
    Optional,
    List,
    ComponentResource,
    ResourceOptions,
    serviceaccount,
    projects,
    config
)

class ServiceAccount(ComponentResource):
    """
    Create a service account and add IAM role membership to it.
    Args:
        name: str, the name of the service account
        description: Optional[str], the description of the service account
        roles: Optional[List[str]], the roles to add to the service account
        display_name: Optional[str], the display name of the service account
        project: Optional[str], the project to create the service account in
        disabled: bool, whether to disable the service account
    """
    def __init__(self, 
                 name: str, 
                 description: Optional[str] = None, 
                 roles: Optional[List[str]] = None,
                 display_name: Optional[str] = None,
                 project: Optional[str] = None,
                 disabled: bool = False,
                 opts: Optional[ResourceOptions] = None):
        self.name = name
        self.description = description
        self.roles = roles
        self.project = project or config.project
        self.opts = opts

        super().__init__('custom:resource:ServiceAccount', name, {}, opts)
        
        self.create_service_account()
        self.add_iam_role_membership()
        # self.create_key()
        
        self.register_outputs({
        'email': self.sa.email,
        'unique_id': self.sa.unique_id,
        'name': self.sa.name,
        'display_name': self.sa.display_name,
        'description': self.sa.description,
    })
    def create_service_account(self):
        self.sa = serviceaccount.Account(
            self.name,
            account_id=self.name,
            display_name=self.name,
            description=self.description,
            opts=self.opts
        )
        
    def add_iam_role_membership(self):
        if self.roles:
            for role in self.roles:
                projects.IAMMember(
                    f"{self.name}-{role}",
                    project=self.project,
                    role=role,
                    member=self.sa.email.apply(
                        lambda email: f"serviceAccount:{email}"
                    ),
                    opts=ResourceOptions(parent=self.sa, depends_on=[self.sa])
                )