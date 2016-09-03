from __future__ import absolute_import, unicode_literals

from binlogsync.event.row_wrapper import InsertEventRow, UpdateEventRow, DeleteEventRow


class IEventHandler(object):
    def on_insert_raw(self, ev_id, ev):
        ev_timestamp = ev.packet.timestamp
        schema = ev.schema
        table = ev.table
        affected_rows = []
        for row in ev.rows:
            affected_rows.append(InsertEventRow(ev_id=ev_id,
                                                ev=ev,
                                                new_values=row['values'], ))
        self.on_insert(
            ev_id, ev_timestamp,
            schema, table,
            affected_rows,
            ev=ev)

    def on_update_raw(self, ev_id, ev):
        ev_timestamp = ev.packet.timestamp
        schema = ev.schema
        table = ev.table
        affected_rows = []
        for row in ev.rows:
            affected_rows.append(UpdateEventRow(
                ev_id=ev_id,
                ev=ev,
                old_values=row['before_values'],
                new_values=row['after_values'], ))
        self.on_update(
            ev_id, ev_timestamp,
            schema, table,
            affected_rows,
            ev=ev)

    def on_delete_raw(self, ev_id, ev):
        ev_timestamp = ev.packet.timestamp
        schema = ev.schema
        table = ev.table
        affected_rows = []
        for row in ev.rows:
            affected_rows.append(DeleteEventRow(ev_id=ev_id,
                                                ev=ev,
                                                old_values=row['values'], ))
        self.on_delete(
            ev_id, ev_timestamp,
            schema, table,
            affected_rows,
            ev=ev)
        pass

    def on_insert(self, ev_id, ev_timestamp, schema, table, affected_rows, ev):
        """
        :param ev_id: unique string id for each event
        :param ev_timestamp: unix epoch for the time event happens
        :param schema: affected database name
        :param table: affected table name
        :param affected_rows: list of instance of binlogsync.event.row_wrapper.InsertEventRow
                              each of instance has an attr named "new_values",
                              which is a dict(whose key is MySQL column name) of the new inserted row

                              for row in affected_rows:
                                do_something(row.new_values)
        """
        pass

    def on_update(self, ev_id, ev_timestamp, schema, table, affected_rows, ev):
        """
        :param ev_id: unique string id for each event
        :param ev_timestamp: unix epoch for the time event happens
        :param schema: affected database name
        :param table: affected table name
        :param affected_rows: list of instance of binlogsync.event.row_wrapper.UpdateEventRow
                              each of instance has two attrs named "new_values" and "old_values",
                              which are dicts(whose key is MySQL column name) of the new inserted row and the old replaced row

                              for row in affected_rows:
                                do_something(row.new_values, row.old_values)
        """
        pass

    def on_delete(self, ev_id, ev_timestamp, schema, table, affected_rows, ev):
        """
        :param ev_id: unique string id for each event
        :param ev_timestamp: unix epoch for the time event happens
        :param schema: affected database name
        :param table: affected table name
        :param affected_rows: list of instance of binlogsync.event.row_wrapper.DeleteEventRow
                              each of instance has an attr named "old_values",
                              which is a dict(whose key is MySQL column name) of the deleted row

                              for row in affected_rows:
                                do_something(row.old_values)
        """
        pass

    def close(self):
        """allow user to release some resource
        """
        pass
