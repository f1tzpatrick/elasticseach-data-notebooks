from jinja2 import Environment, FileSystemLoader, Template

def jinja_env(template_dir="../templates"):
    loader = FileSystemLoader(searchpath="../templates")
    return Environment(loader=loader)

def make_alias_request(alias, kind, items):
    template = jinja_env().get_template("alias.j2")
    return template.render(alias=alias, kind=kind, items=items)

def make_index_curator_job(job, repo, alias):
    template = jinja_env().get_template("curator_action.j2")
    return template.render(job=job, repo=repo, action=action, alias=alias)

def make_snapshot_curator_job(job, repo, keep_snapshots_regex):
    template = jinja_env().get_template("curator_action.j2")
    return template.render(job=job, repo=repo, action=action, alias=alias)