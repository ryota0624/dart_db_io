import 'package:dart_db_io/src/db_io.dart';
import 'package:dart_db_io/src/firestore_io.dart';
import 'package:fpdart/fpdart.dart';
import 'package:test/test.dart';
import 'dart:math';

import 'package:dart_firebase_admin/firestore.dart';

import 'firestore.dart';

void main() {
  group('Firestore', () {
    late Firestore firestore;
    late MessageRepositoryImpl repositoryImpl;
    late FirestoreDatabaseIORunner firestoreDatabaseIORunner;

    setUp(() {
      firestore = createFirestore();
      firestoreDatabaseIORunner = FirestoreDatabaseIORunner(firestore);
      repositoryImpl = MessageRepositoryImpl();
    });

    test('store', () async {
      final message = '${Random().nextInt(
        1000000,
      )}_Hello, World!';
      final dbio = repositoryImpl.store(message);
      await firestoreDatabaseIORunner.runWriteSession(dbio);
    });

    test('read', () async {
      final message = '${Random().nextInt(
        1000000,
      )}_Hello, World!';
      {
        final dbio = repositoryImpl.store(message);
        await firestoreDatabaseIORunner.runWriteSession(dbio);
      }

      {
        final dbio = repositoryImpl.getById(message);
        final result = await firestoreDatabaseIORunner.runReadSession(dbio);
        expect(result.getRight().toNullable()?.toNullable(), message);
      }
    });

    test('read & write', () async {
      final message = '${Random().nextInt(
        1000000,
      )}_Hello, World!';
      {
        final dbio = repositoryImpl.store(message);
        await firestoreDatabaseIORunner.runWriteSession(dbio);
      }

      {
        final dbio = repositoryImpl
            .getById(message)
            .withWriteSession()
            .map((message) => "${message}_updated")
            .flatMap((updated) {
          return repositoryImpl.store(updated);
        });
        await firestoreDatabaseIORunner.runWriteSession(dbio);
      }
      {
        final readSession = ReadFirestoreSession(firestore, null);
        final result = await repositoryImpl.getById(message).run(readSession);
        expect(result.toNullable()?.toNullable(), message);
      }
    });
  });
}

typedef RepositoryIO<DB, V> = DatabaseIO<DB, (), V>;

abstract interface class MessageRepository<DB> {
  RepositoryIO<DB, Option<String>> getById(String pickingId);

  RepositoryIO<DB, ()> store(String message);
}

class MessageRepositoryImpl implements MessageRepository<FirestoreSession> {
  @override
  DatabaseIO<ReadFirestoreSession, (), Option<String>> getById(
    String message,
  ) {
    return DatabaseIO.safe((session) async {
      final ref = session.firestore.collection('messages').doc(
            message,
          );
      final msg = (await ref.get()).data()?['message'] as String?;
      return optionOf(msg);
    });
  }

  @override
  DatabaseIO<WriteFirestoreSession, (), ()> store(
    String message,
  ) {
    return DatabaseIO.safe((session) async {
      session.transaction.create(
          session.transaction.firestore.collection('messages').doc(
                message,
              ),
          {
            'message': message,
          });
      return ();
    });
  }
}
