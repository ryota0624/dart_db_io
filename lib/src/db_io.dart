import 'package:fpdart/fpdart.dart';

abstract final class _DatabaseIOHKT {}

class DatabaseIO<Session, Err, V> extends HKT2<_DatabaseIOHKT, Err, V>
    with
        Functor2<_DatabaseIOHKT, Err, V>,
        Applicative2<_DatabaseIOHKT, Err, V>,
        Monad2<_DatabaseIOHKT, Err, V> {
  final Future<Either<Err, V>> Function(Session session) _run;

  static DatabaseIO<Session, List<Err>, List<V>> traverseList<Session, Err, V>(
      List<DatabaseIO<Session, Err, V>> list) {
    return DatabaseIO(
      (session) async {
        final results =
            await Future.wait(list.map((dbio) => dbio.run(session)));
        if (results.every((result) => result.isRight())) {
          return Either.right(results
              .map(
                (result) => result
                    .getOrElse((err) => throw Exception('unexpected: $err')),
              )
              .toList());
        } else {
          return Either.left(
            results
                .where((result) => result.isLeft())
                .map((result) => result.swap().getOrElse((_) => throw Error()))
                .toList(),
          );
        }
      },
    );
  }

  DatabaseIO(this._run);

  factory DatabaseIO.safe(Future<V> Function(Session session) run) {
    return DatabaseIO((session) async {
      return Either.right(await run(session));
    });
  }

  @override
  DatabaseIO<Session, Err, C> pure<C>(C c) =>
      DatabaseIO((_) => Future.value(Either.right(c)));

  @override
  DatabaseIO<Session, Err, B> flatMap<B>(
      covariant DatabaseIO<Session, Err, B> Function(V a) f) {
    return DatabaseIO(
      (session) => run(session).then((resolved) {
        return resolved.fold(
          (err) => Future.value(Either.left(err)),
          (resolved) => f(resolved).run(session),
        );
      }),
    );
  }

  @override
  DatabaseIO<Session, Err, B> map<B>(B Function(V a) f) {
    return DatabaseIO((session) => run(session).then((resolved) {
          return resolved.map(f);
        }));
  }

  DatabaseIO<Session, C, V> mapError<C>(C Function(Err a) f) {
    return DatabaseIO((session) => run(session).then((resolved) {
          return resolved.mapLeft(f);
        }));
  }

  Future<Either<Err, V>> run(Session session) {
    return _run(session);
  }
}

extension EitherDatabaseIOExtension<Err, V> on Either<Err, V> {
  DatabaseIO<Session, Err, V> toDatabaseIO<Session>() {
    return DatabaseIO((_) async {
      return this;
    });
  }
}

extension TaskEitherDatabaseIOExtension<Err, V> on TaskEither<Err, V> {
  DatabaseIO<Session, Err, V> toDatabaseIO<Session>() {
    return DatabaseIO((_) async {
      return run();
    });
  }
}

extension OptionDatabaseIOExtension<V> on Option<V> {
  DatabaseIO<Session, Err, V> toDatabaseIO<Session, Err>({
    required Err Function() orElse,
  }) {
    return DatabaseIO((_) async {
      return fold(
        () => Either.left(orElse()),
        (v) => Either.right(v),
      );
    });
  }
}
