from sanic.exceptions import SanicException, ServerError
from sanic.response import json, text, HTTPResponse

class IllegalArgumentError(Exception):
    pass

class ProcessingException(SanicException):
    """Raised when a preprocess or postprocess encounters a problem.

    This exception should be raised by functions supplied in the
    ``preprocess`` and ``postprocess`` keyword arguments to
    :class:`APIManager.create_api`. When this exception is raised, all
    preprocessing or postprocessing halts, so any processors appearing later in
    the list will not be invoked.

    `code` is the HTTP status code of the response supplied to the client in
    the case that this exception is raised. `description` is an error message
    describing the cause of this exception. This message will appear in the
    JSON object in the body of the response to the client.

    """
    def __init__(self, message='', status_code=520):
        super(ProcessingException, self).__init__(message, status_code)
        self.status_code = status_code
        self.message = message

def response_exception(exception):
    if type(exception.message) is dict:
        return json(exception.message, status=exception.status_code)
    else:
        return text(exception.message, status=exception.status_code)


class ValidationError(SanicException):
    """Raised when there is a problem deserializing a dictionary into an
    instance of a SQLAlchemy model.

    """
    pass
