class EventRow(object):
    def __init__(self, ev_id, ev):
        self.ev_id = ev_id
        self.ev = ev


class InsertEventRow(EventRow):
    def __init__(self, ev_id, ev, new_values):
        super(InsertEventRow, self).__init__(ev_id, ev)
        self.new_values = new_values


class UpdateEventRow(EventRow):
    def __init__(self, ev_id, ev, old_values, new_values):
        super(UpdateEventRow, self).__init__(ev_id, ev)
        self.old_values = old_values
        self.new_values = new_values


class DeleteEventRow(EventRow):
    def __init__(self, ev_id, ev, old_values):
        super(DeleteEventRow, self).__init__(ev_id, ev)
        self.old_values = old_values
