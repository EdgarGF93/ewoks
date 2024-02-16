broker_url = "redis://localhost:6379/3"
result_backend = "redis://localhost:6379/4"

result_serializer = "pickle"
accept_content = [
    "application/json",
    "application/x-python-serialize",
]
result_expires = 600
task_remote_tracebacks = True