module.controller('PasteQueryCtrl', ['$scope', '$modalInstance', SimplePasteQueryCtrl]);

function SimplePasteQueryCtrl($scope, $modalInstance) {

	$scope.ok = function () {
		var error = $scope.validate();

		if (error) {
			$scope.alert(error);
		}
		else {
			var result = {};
			result.query = JSON.parse($scope.query);
			$modalInstance.close(result);
		}
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