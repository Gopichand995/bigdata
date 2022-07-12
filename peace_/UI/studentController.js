mainApp.controller("studentController", function($scope){
    $scope.student = {
        firstName: Gopichand,
        lastName: Barri,
        fees: 10000,
        subjects: [
            {name: 'Physics', marks: 70},
            {name: 'Maths', marks: 70},
            {name: 'Chemistry', marks: 70},
            {name: 'English', marks: 70},
            {name: 'Hindi', marks: 70},
        ],
        fullName: function(){
            var studentObject;
            studentObject = $scope.student;
            return studentObject.firstName + " " + studentObject.lastName;
        }
    };
});