class pipeline:

    def __init__(self, name: str, process: [], error_behavior=[], vb=True, protected=True, allow_crash_on_unanticipated=True, error_return_object=None):
        """Creating a pipeline for easy processing of data"""
        self.name, self.process = name, process
        self.vb, self.allow_crash_on_unanticipated = vb, allow_crash_on_unanticipated
        self.vars = dict()
        # Error handling disables
        if len(error_behavior) == 0 or not protected:
            self.error_handling = False
        # Error handling enabled
        else:
            if len(process) != len(error_behavior):
                raise AttributeError('Error behavior list size mismatch')
            if error_return_object == None:
                raise AttributeError('No error return object')
            self.error_return_object = error_return_object
            self.error_handling = True
            self.error_behavior = error_behavior
        if self.vb:
            print("Pipeline Created: [ Name: {name}, Process_length: {pl}, Error_handling_enabled: {eh}, Allow_unanticipated_crash: {ac} ]".format(name=self.name, pl=len(self.process), eh=self.error_handling, ac=self.allow_crash_on_unanticipated))

    def import_var(self, vars: dict):
        """Imports variable to pipeline"""
        self.vars.update(vars)

    def process_item(self, item, dev_mode=False):
        """Passes an item through constructed pipeline"""
        for step_index, func in enumerate(self.process):
            if self.error_handling:
                # Run with protection
                try:
                    item = func(item)
                except Exception as e:
                    # Fetch error handling instruction
                    error_handling_instr = self.error_behavior[step_index]
                    # Print message
                    if self.vb:
                        print("Pipeline {name} encountered exception at stage [{stg}] : {e} {args}".format(stg=func.__name__, name=self.name, e=type(e).__name__, args=e.args[0]), end='')
                    # Handle all error types
                    if error_handling_instr == 'all':
                        if self.vb:
                            print(" Result: handled according to instruction: all")
                    # Handle partial error types
                    elif type(error_handling_instr) == list:
                        # If error is instructed to skip
                        if type(e) in error_handling_instr:
                            print(" Result: handled according to instruction to handle type: {e_type}".format(e_type=type(e)))
                        # If not allowed to crash on unanticipated errors
                        elif not self.allow_crash_on_unanticipated:
                            print(" Result: handled due to not allowing crash on unanticipated")
                        # If allows for crash, stop operation
                        else:
                            print(" Result: Crashing due to unprotected type {e_type}".format(e_type=type(e)))
                            raise e
                    # No handling instruction
                    elif error_handling_instr == None:
                        if self.allow_crash_on_unanticipated:
                            print(" Result: Crashing due to no protection strategy")
                            raise e
                        else:
                            print(" Result: handled due to not allowing crash on unanticipated")
                    # Invalid input
                    else:
                        if self.allow_crash_on_unanticipated:
                            raise AttributeError("Invalid error handling instruction: {instr}".format(instr=error_handling_instr))
                        else:
                            print(" Result: Invalid error handling instruction: {instr}, Please fix immediately!".format(instr=error_handling_instr))
                    # Return default item
                    return self.error_return_object
            else:
                # Run in regular mode
                item = func(item)
            if dev_mode:
                print("Stage #{stg} Operation: {op} Result:\n{result}".format(stg=step_index, op=func.__name__, result=item))
        return item