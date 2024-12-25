import 'package:dart_firebase_admin/firestore.dart';
import 'package:fpdart/fpdart.dart';

import 'db_io.dart';

sealed class FirestoreSession {
  static TaskEither<Err, V> runReadSession<Err, V>(
    Firestore firestore,
    DatabaseIO<ReadFirestoreSession, Err, V> dbio,
  ) {
    final session = ReadFirestoreSession.readOnly(firestore);
    return TaskEither(() {
      return dbio.run(session);
    });
  }

  static TaskEither<Err, V> runWriteSession<Err, V>(
    Firestore firestore,
    DatabaseIO<WriteFirestoreSession, Err, V> dbio,
  ) {
    return TaskEither(() async {
      final tx = await firestore.beginTransaction();
      final session = WriteFirestoreSession(tx);
      final result = await dbio.run(
        session,
      );

      try {
        await tx.commit();
      } catch (e) {
        await tx.rollback();
        rethrow;
      }
      return result;
    });
  }
}

class WriteFirestoreSession implements FirestoreSession {
  final Transaction transaction;

  WriteFirestoreSession(this.transaction);

  ReadFirestoreSession asReadSession() {
    return ReadFirestoreSession(transaction.firestore, transaction);
  }
}

class ReadFirestoreSession implements FirestoreSession {
  final Firestore firestore;
  final Transaction? transaction;

  ReadFirestoreSession(this.firestore, this.transaction);

  ReadFirestoreSession.readOnly(
    this.firestore,
  ) : transaction = null;
}

extension ReadFirestoreSessionExtension<Err, V>
    on DatabaseIO<ReadFirestoreSession, Err, V> {
  DatabaseIO<WriteFirestoreSession, Err, V> withWriteSession() {
    return DatabaseIO<WriteFirestoreSession, Err, V>((session) async {
      return await run(
        session.asReadSession(),
      );
    });
  }
}

class FirestoreDatabaseIORunner {
  final Firestore _firestore;

  FirestoreDatabaseIORunner(this._firestore);

  Future<Either<Err, V>> runWriteSession<Err, V>(
      DatabaseIO<WriteFirestoreSession, Err, V> dbio) async {
    final tx = await _firestore.beginTransaction();
    final session = WriteFirestoreSession(tx);
    final result = await dbio.run(session);
    return await result.match(
      (_) async {
        await tx.rollback();
        return result;
      },
      (_) async {
        await tx.commit();
        return result;
      },
    );
  }

  Future<Either<Err, V>> runReadSession<Err, V>(
      DatabaseIO<ReadFirestoreSession, Err, V> dbio) async {
    final session = ReadFirestoreSession.readOnly(_firestore);
    final result = await dbio.run(session);
    return result;
  }
}
