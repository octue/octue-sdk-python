class OctueSDKException(Exception):
    """All exceptions from this library inherit from here"""


class InvalidInputException(OctueSDKException, ValueError):
    """Raise when an object is instantiated or a function called with invalid inputs"""


class FileNotFoundException(InvalidInputException, FileNotFoundError):
    """Raise when a required folder (e.g. <data_dir>/input) cannot be found"""


class FileLocationError(Exception):
    """Raise when a file exists in an unsupported location for a given operation."""


class ProtectedAttributeException(OctueSDKException, KeyError):
    """Raise when a user attempts to set an attribute whose value should be protected"""


class FolderNotFoundException(InvalidInputException):
    """Raise when a multi manifest can not be refined to a single manifest in a search"""


class ManifestNotFoundException(InvalidInputException):
    """Raise when a multi manifest can not be refined to a single manifest in a search"""


class InvalidManifestException(InvalidInputException):
    """Raise when a manifest loaded from JSON does not pass validation"""


class InvalidManifestTypeException(InvalidManifestException):
    """Raised when user attempts to create a manifest of a type other than 'input', 'output' or 'build'"""


class InvalidOctueFileTypeException(OctueSDKException):
    """Raised when you attempt to register a file type in the results manifest that Octue doesn't know about"""


class InvalidFilePointerException(OctueSDKException):
    """Raised when you attempt to create a Datafile with an object that is not file-like."""


class NotImplementedYetException(OctueSDKException):
    """Raised when you attempt to use a function whose high-level API is in place, but which is not implemented yet"""


class UnexpectedNumberOfResultsException(OctueSDKException):
    """Raise when searching for a single data file (or a particular number of data files) and the number of results
    exceeds that expected
    """


class BrokenSequenceException(OctueSDKException):
    """Raise when filtering for a sequence of files whose numbering does not start at 0, or does not monotonically
    increase
    """


class InvalidTagException(OctueSDKException, ValueError):
    """Raise when a tag applied to a data file or dataset"""


class InvalidLabelException(OctueSDKException, ValueError):
    """Raise when a label applied to a data file or dataset"""


class ServiceNotFound(OctueSDKException):
    """Raise when a Service of the given ID has not been found on the Google Pub/Sub server (i.e. if there is no topic
    associated with the Service ID).
    """


class QuestionNotDelivered(OctueSDKException):
    """Raise if a question has not been delivered or no acknowledgement of its delivery has been received."""


class BackendNotFound(OctueSDKException):
    """Raise when details of a backend that doesn't exist in `octue.resources.service_backends` are given for use as a
    Service backend.
    """


class AttributeConflict(OctueSDKException):
    """Raise if, when trying to set an attribute whose current value has a significantly higher confidence than the new
    value, the new value conflicts with the current value.
    """


class MissingServiceID(OctueSDKException):
    """Raise when a specific ID for a service is expected to be provided, but is missing or None."""


class CloudLocationNotSpecified(OctueSDKException):
    """Raise when attempting to interact with a cloud resource implicitly but the implicit details of its location are
    missing.
    """


class InvalidMonitorMessage(OctueSDKException):
    """Raise if a monitor message fails validation against the "monitor_message_schema" field of the Twine."""


class DeploymentError(OctueSDKException):
    """Raise if an error occurs during the deployment of an octue service."""


class CloudStorageBucketNotFound(OctueSDKException):
    """Raise if attempting to access a cloud storage bucket that cannot be found."""
