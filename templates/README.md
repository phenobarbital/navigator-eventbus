# templates/

Required by `notify` (async-notify, extra `[notify]`): `notify.notify`
builds a Jinja2 `TemplateParser` at import time over a `templates/`
directory relative to the process's current working directory, and
raises `RuntimeError` if it does not exist.

This directory exists so `import notify` (and therefore
`NotificationSubscriber`'s default sender) does not crash for consumers
of the `[notify]` extra who have not defined their own email templates.
Add `.html`/`.txt` templates here if you use `notify`'s templated email
provider; otherwise this placeholder is sufficient.
