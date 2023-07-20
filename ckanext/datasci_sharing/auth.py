from ckan.plugins import toolkit


@toolkit.auth_allow_anonymous_access
def share_internally_show(context, package):
    # only users who can update this field can view the value for it.
    return share_internally_update(context, package)

@toolkit.auth_allow_anonymous_access
def share_internally_update(context, package):
    # only users who can update this package can update the value for this field.
    try:
        toolkit.check_access('package_update', context, package)
        return {'success': True}
    except toolkit.NotAuthorized:
        return {'success': False, 'msg': f'User {context.get("user")} not authorized'}
