"""
DQ Configuration Validator
Validates DQ JSON configuration files before deployment to catch errors early

Usage:
    python validate_dq_config.py <path_to_json_file>
    python validate_dq_config.py sample_dq_config.json
    python validate_dq_config.py gs://bucket/path/to/config.json

Exit Codes:
    0 - Validation passed
    1 - Validation failed
"""

import sys
import json
import argparse
from typing import List, Dict, Any, Tuple
from pathlib import Path


class DQConfigValidator:
    """Validates DQ configuration JSON files"""
    
    # Valid values for validation
    VALID_SEVERITIES = ["High", "Medium", "Low"]
    VALID_COMPARISON_TYPES = ["numeric_condition", "set_match", "not_in_result", "row_match"]
    REQUIRED_FIELDS = [
        "check_id", "category", "sql_query", "description", 
        "severity", "expected_output", "comparison_type", "active"
    ]
    OPTIONAL_FIELDS = ["impacted_downstream", "tags"]
    
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.total_checks = 0
        self.valid_checks = 0
        self.invalid_checks = 0
    
    def validate_file(self, file_path: str) -> bool:
        """
        Validate DQ configuration file
        
        Args:
            file_path: Path to JSON file (local or GCS)
            
        Returns:
            True if validation passed, False otherwise
        """
        print("=" * 80)
        print("DQ CONFIGURATION VALIDATOR")
        print("=" * 80)
        print(f"File: {file_path}")
        print()
        
        # Read JSON file
        try:
            json_data = self._read_json_file(file_path)
        except Exception as e:
            self._add_error(f"Failed to read JSON file: {str(e)}")
            return False
        
        # Validate JSON structure
        if not self._validate_json_structure(json_data):
            return False
        
        # Validate each check
        self.total_checks = len(json_data)
        print(f"Total checks to validate: {self.total_checks}")
        print()
        
        for index, check in enumerate(json_data):
            self._validate_check(check, index)
        
        # Print results
        self._print_results()
        
        return len(self.errors) == 0
    
    def _read_json_file(self, file_path: str) -> List[Dict]:
        """Read JSON file from local path or GCS"""
        if file_path.startswith('gs://'):
            # For GCS files, user needs to have gcloud configured
            # This is a simple implementation - production code should use google-cloud-storage
            raise ValueError(
                "GCS paths are not supported in this standalone validator. "
                "Please download the file locally first or use the pipeline's validation."
            )
        
        # Read local file
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _validate_json_structure(self, json_data: Any) -> bool:
        """Validate basic JSON structure"""
        if not isinstance(json_data, list):
            self._add_error("JSON file must contain an array at the root level")
            return False
        
        if len(json_data) == 0:
            self._add_error("JSON file cannot be empty")
            return False
        
        return True
    
    def _validate_check(self, check: Dict, index: int) -> None:
        """Validate a single DQ check"""
        check_id = check.get('check_id', f'UNKNOWN_CHECK_{index}')
        
        print(f"[{index + 1}/{self.total_checks}] Validating: {check_id}")
        
        check_errors = []
        check_warnings = []
        
        # Validate it's a dictionary
        if not isinstance(check, dict):
            self._add_error(f"Check at index {index} must be a dictionary, got {type(check).__name__}", check_id)
            self.invalid_checks += 1
            return
        
        # Validate required fields
        missing_fields = []
        for field in self.REQUIRED_FIELDS:
            if field not in check:
                missing_fields.append(field)
        
        if missing_fields:
            check_errors.append(f"Missing required fields: {', '.join(missing_fields)}")
        
        # Validate check_id
        if 'check_id' in check:
            if not isinstance(check['check_id'], str) or not check['check_id'].strip():
                check_errors.append("check_id must be a non-empty string")
        
        # Validate category
        if 'category' in check:
            if not isinstance(check['category'], str) or not check['category'].strip():
                check_errors.append("category must be a non-empty string")
        
        # Validate sql_query
        if 'sql_query' in check:
            if not isinstance(check['sql_query'], str) or not check['sql_query'].strip():
                check_errors.append("sql_query must be a non-empty string")
        
        # Validate description
        if 'description' in check:
            if not isinstance(check['description'], str):
                check_errors.append("description must be a string")
        
        # Validate severity
        if 'severity' in check:
            severity_errors = self._validate_severity(check['severity'])
            check_errors.extend(severity_errors)
        
        # Validate comparison_type and expected_output
        if 'comparison_type' in check and 'expected_output' in check:
            comparison_errors = self._validate_comparison_type(
                check['comparison_type'], 
                check['expected_output']
            )
            check_errors.extend(comparison_errors)
        
        # Validate active flag
        if 'active' in check:
            if not isinstance(check['active'], bool):
                check_errors.append(f"active must be a boolean, got {type(check['active']).__name__}")
        
        # Validate optional fields
        if 'impacted_downstream' in check:
            if not isinstance(check['impacted_downstream'], list):
                check_warnings.append(f"impacted_downstream should be a list, got {type(check['impacted_downstream']).__name__}")
        
        if 'tags' in check:
            if not isinstance(check['tags'], list):
                check_warnings.append(f"tags should be a list, got {type(check['tags']).__name__}")
        
        # Check for unknown fields (not an error, just a warning)
        known_fields = self.REQUIRED_FIELDS + self.OPTIONAL_FIELDS
        unknown_fields = [field for field in check.keys() if field not in known_fields]
        if unknown_fields:
            check_warnings.append(f"Unknown fields (will be ignored): {', '.join(unknown_fields)}")
        
        # Record results
        if check_errors:
            self.invalid_checks += 1
            for error in check_errors:
                self._add_error(error, check_id)
            print(f"  ✗ INVALID - {len(check_errors)} error(s)")
        else:
            self.valid_checks += 1
            print(f"  ✓ VALID")
        
        if check_warnings:
            for warning in check_warnings:
                self._add_warning(warning, check_id)
            print(f"  ⚠ {len(check_warnings)} warning(s)")
    
    def _validate_severity(self, severity: Any) -> List[str]:
        """Validate severity value"""
        errors = []
        
        if not isinstance(severity, str):
            errors.append(f"severity must be a string, got {type(severity).__name__}")
            return errors
        
        if severity not in self.VALID_SEVERITIES:
            errors.append(
                f"Invalid severity '{severity}'. Must be one of: {', '.join(self.VALID_SEVERITIES)}"
            )
        
        return errors
    
    def _validate_comparison_type(self, comparison_type: Any, expected_output: Any) -> List[str]:
        """Validate comparison_type and expected_output compatibility"""
        errors = []
        
        # Validate comparison_type is valid
        if not isinstance(comparison_type, str):
            errors.append(f"comparison_type must be a string, got {type(comparison_type).__name__}")
            return errors
        
        if comparison_type not in self.VALID_COMPARISON_TYPES:
            errors.append(
                f"Invalid comparison_type '{comparison_type}'. "
                f"Must be one of: {', '.join(self.VALID_COMPARISON_TYPES)}"
            )
            return errors
        
        # Validate expected_output format based on comparison_type
        if comparison_type == "numeric_condition":
            if not isinstance(expected_output, str):
                errors.append(
                    f"For comparison_type 'numeric_condition', expected_output must be a string, "
                    f"got {type(expected_output).__name__}"
                )
            else:
                # Validate it's a valid numeric condition
                expected_output_stripped = expected_output.strip()
                if not expected_output_stripped:
                    errors.append("For comparison_type 'numeric_condition', expected_output cannot be empty")
                else:
                    # Check if it's a valid number or condition
                    operators = [">=", "<=", "==", "!=", ">", "<"]
                    has_operator = any(expected_output_stripped.startswith(op) for op in operators)
                    
                    if has_operator:
                        # Extract numeric part after operator
                        for op in operators:
                            if expected_output_stripped.startswith(op):
                                numeric_part = expected_output_stripped[len(op):].strip()
                                try:
                                    float(numeric_part)
                                except ValueError:
                                    errors.append(
                                        f"For comparison_type 'numeric_condition', expected_output '{expected_output}' "
                                        f"has invalid numeric value after operator"
                                    )
                                break
                    else:
                        # Should be a plain number
                        try:
                            float(expected_output_stripped)
                        except ValueError:
                            errors.append(
                                f"For comparison_type 'numeric_condition', expected_output '{expected_output}' "
                                f"must be a valid number or condition (e.g., '0', '>=10')"
                            )
        
        elif comparison_type in ["set_match", "not_in_result"]:
            if not isinstance(expected_output, list):
                errors.append(
                    f"For comparison_type '{comparison_type}', expected_output must be a list, "
                    f"got {type(expected_output).__name__}"
                )
            elif len(expected_output) == 0:
                errors.append(
                    f"For comparison_type '{comparison_type}', expected_output list cannot be empty"
                )
        
        elif comparison_type == "row_match":
            if not isinstance(expected_output, list):
                errors.append(
                    f"For comparison_type 'row_match', expected_output must be a list, "
                    f"got {type(expected_output).__name__}"
                )
            elif len(expected_output) == 0:
                errors.append("For comparison_type 'row_match', expected_output list cannot be empty")
            else:
                # Validate each item is a dictionary
                for idx, item in enumerate(expected_output):
                    if not isinstance(item, dict):
                        errors.append(
                            f"For comparison_type 'row_match', expected_output must be a list of objects. "
                            f"Item at index {idx} is {type(item).__name__}, not a dictionary"
                        )
                        break
        
        return errors
    
    def _add_error(self, message: str, check_id: str = None) -> None:
        """Add an error message"""
        if check_id:
            self.errors.append(f"[{check_id}] {message}")
        else:
            self.errors.append(message)
    
    def _add_warning(self, message: str, check_id: str = None) -> None:
        """Add a warning message"""
        if check_id:
            self.warnings.append(f"[{check_id}] {message}")
        else:
            self.warnings.append(message)
    
    def _print_results(self) -> None:
        """Print validation results"""
        print()
        print("=" * 80)
        print("VALIDATION RESULTS")
        print("=" * 80)
        print(f"Total checks: {self.total_checks}")
        print(f"Valid checks: {self.valid_checks}")
        print(f"Invalid checks: {self.invalid_checks}")
        print(f"Errors: {len(self.errors)}")
        print(f"Warnings: {len(self.warnings)}")
        print()
        
        if self.errors:
            print("=" * 80)
            print("ERRORS")
            print("=" * 80)
            for i, error in enumerate(self.errors, 1):
                print(f"{i}. {error}")
            print()
        
        if self.warnings:
            print("=" * 80)
            print("WARNINGS")
            print("=" * 80)
            for i, warning in enumerate(self.warnings, 1):
                print(f"{i}. {warning}")
            print()
        
        if len(self.errors) == 0:
            print("=" * 80)
            print("✓ VALIDATION PASSED")
            print("=" * 80)
            print("All checks are valid and ready for deployment!")
        else:
            print("=" * 80)
            print("✗ VALIDATION FAILED")
            print("=" * 80)
            print(f"Found {len(self.errors)} error(s). Please fix them before deployment.")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Validate DQ configuration JSON files before deployment',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python validate_dq_config.py sample_dq_config.json
  python validate_dq_config.py Extra/sample_dq_config.json
  python validate_dq_config.py sample_dq_config_negative_tests.json

Exit Codes:
  0 - Validation passed (ready for deployment)
  1 - Validation failed (fix errors before deployment)
        """
    )
    
    parser.add_argument(
        'json_file',
        help='Path to DQ configuration JSON file'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed validation information'
    )
    
    args = parser.parse_args()
    
    # Check if file exists
    if not args.json_file.startswith('gs://'):
        if not Path(args.json_file).exists():
            print(f"Error: File not found: {args.json_file}")
            sys.exit(1)
    
    # Validate configuration
    validator = DQConfigValidator()
    
    try:
        validation_passed = validator.validate_file(args.json_file)
        
        # Exit with appropriate code
        sys.exit(0 if validation_passed else 1)
        
    except Exception as e:
        print(f"Error: Validation failed with exception: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
