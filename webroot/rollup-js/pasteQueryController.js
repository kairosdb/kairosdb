module.controller('PasteQueryCtrl', ['$scope', '$modalInstance', PasteQueryCtrl]);

function PasteQueryCtrl($scope, $modalInstance) {

    $scope.EXECUTION_TYPES = ["Every Minute", "Hourly", "Daily", "Weekly", "Monthly", "Yearly"];
	$scope.executionType = $scope.EXECUTION_TYPES[2];

	$scope.ok = function () {
		var error = $scope.validate();

		if (error) {
			$scope.alert(error);
		}
		else {
			var result = {};
			result.name = $scope.name;
			result.executionType = $scope.executionType;
			result.query = JSON.parse($scope.query);
			$modalInstance.close(result);
		}
	};

	$scope.setExecution = function (type) {
		$scope.executionType = type;
	};

	$scope.cancel = function () {
		$modalInstance.dismiss('cancel');
	};

	// todo duplicated in createController.js
	$scope.alert = function (message, status, data) {
		if (status) {

			var error = "";
			if (data && data.errors)
				error = data.errors;

			bootbox.alert({
				title: message,
				message: status + ":" + (error ? error : "" )
			});
		}
		else {
			bootbox.alert({
				message: message
			});
		}
	};

	$scope.hasErrors = function () {
		return !_.isEmpty($scope.validate());
	};

	$scope.validate = function () {
		if (!$scope.name || $scope.name.length < 1) {
			return "You must specify a name."
		}
		if (!$scope.query || $scope.query.length < 1) {
			return "You must paste in a query.";
		}
		else {
			try {
				JSON.parse($scope.query);
			}
			catch (err) {
				return "Invalid JSON";
			}
		}
		return "";
	}
}